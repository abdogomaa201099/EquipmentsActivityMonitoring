"""
CV Service — Equipment Detection, Tracking, Motion Analysis & Activity Classification.

Processes construction video frame-by-frame:
  1. Object detection via Roboflow (excavators, dump trucks, loaders)
  2. Multi-object tracking with IoU matching + HSV histogram re-identification
  3. Region-based optical flow for ACTIVE/INACTIVE classification
     (handles articulated motion by splitting bounding boxes into sub-regions)
  4. Activity classification: Digging, Swinging, Dumping, Idle
  5. Streams results to Kafka for downstream analytics
"""

import os
import json
import time
import cv2
import numpy as np
import tempfile
from inference_sdk import InferenceHTTPClient
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor



# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
VIDEO_SOURCE = os.environ.get("VIDEO_SOURCE", "videos/sample.mp4")

ROBOFLOW_API_URL = os.environ.get("ROBOFLOW_API_URL", "https://serverless.roboflow.com")
ROBOFLOW_API_KEY = os.environ.get("ROBOFLOW_API_KEY", "3GPqFHWy4SNHqmgqGCI6")
ROBOFLOW_MODEL_ID = os.environ.get("ROBOFLOW_MODEL_ID", "excavators-czvg9/1")
ROBOFLOW_EVERY_N_FRAMES = 10

# Motion thresholds
MOTION_RATIO_THRESHOLD = 1.1   # equipment-to-background motion ratio for ACTIVE
MOTION_MIN_ABSOLUTE = 0.3      # minimum absolute motion (filters sensor noise)

# Activity classification thresholds
SWING_HORIZONTAL_RATIO = 1.5   # |dx|/|dy| above this → horizontal (swing)
SWING_MID_MIN_RATIO = 1.2      # mid-region ratio needed to confirm swing

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "equipment-events")
KAFKA_SEND_EVERY_N_FRAMES = 10

EVENTS_FILE = os.environ.get("EVENTS_FILE", "output/events.json")

# Optical flow performance: compute at reduced resolution, every N frames
FLOW_SCALE = float(os.environ.get("FLOW_SCALE", "0.5"))
FLOW_EVERY_N_FRAMES = int(os.environ.get("FLOW_EVERY_N_FRAMES", "2"))


# ---------------------------------------------------------------------------
# Tracking — IoU matching + HSV histogram re-identification
# ---------------------------------------------------------------------------

class Track:
    """Represents one tracked object."""
    _count = 0

    def __init__(self, bbox, label, histogram=None):
        Track._count += 1
        self.id = Track._count
        self.bbox = list(bbox)
        self.label = label
        self.histogram = histogram
        self.hits = 1
        self.time_since_update = 0

    def update(self, bbox, histogram=None):
        """Update track with a new matched detection."""
        self.bbox = list(bbox)
        self.hits += 1
        self.time_since_update = 0
        if histogram is not None:
            if self.histogram is not None:
                # Exponential moving average to handle appearance drift
                self.histogram = 0.7 * self.histogram + 0.3 * histogram
            else:
                self.histogram = histogram.copy()


class SimpleTracker:
    """
    Greedy IoU tracker with HSV histogram re-identification.

    Matching pipeline:
      1. Greedy IoU matching (same-class, best-IoU-first) on active tracks
      2. Histogram sanity check rejects matches across scene cuts
      3. Unmatched detections attempt re-ID against lost track pool
         (HSV histogram correlation, with class-based fallback)
      4. Remaining detections spawn new tracks (confirmed after min_hits)
    """

    def __init__(self, max_age=200, min_hits=3, iou_threshold=0.15,
                 reid_threshold=0.2):
        self.max_age = max_age
        self.min_hits = min_hits
        self.iou_threshold = iou_threshold
        self.reid_threshold = reid_threshold
        self.active_tracks = []
        self.lost_tracks = []
        Track._count = 0

    def update(self, detections, frame=None):
        """Match new detections to existing tracks; assigns equipment_id to each."""
        # Age all active tracks
        for trk in self.active_tracks:
            trk.time_since_update += 1

        if not detections:
            for trk in self.active_tracks:
                if trk.hits >= self.min_hits:
                    self.lost_tracks.append(trk)
            self.active_tracks = []
            self.lost_tracks = [
                t for t in self.lost_tracks if t.time_since_update <= self.max_age
            ]
            return detections

        histograms = []
        for det in detections:
            histograms.append(
                self._compute_histogram(frame, det["bbox"]) if frame is not None else None
            )

        # Greedy IoU matching (same-class, best-IoU-first)
        matched_track_set = set()
        matched_det_set = set()
        pairs = []
        for t_idx, trk in enumerate(self.active_tracks):
            for d_idx, det in enumerate(detections):
                if trk.label != det["label"]:
                    continue
                iou = self._iou(trk.bbox, det["bbox"])
                if iou >= self.iou_threshold:
                    pairs.append((iou, t_idx, d_idx))
        pairs.sort(reverse=True)

        for iou, t_idx, d_idx in pairs:
            if t_idx in matched_track_set or d_idx in matched_det_set:
                continue
            trk = self.active_tracks[t_idx]
            hist = histograms[d_idx]
            # Reject match if histogram correlation is too low (scene cut)
            if trk.histogram is not None and hist is not None:
                corr = cv2.compareHist(hist, trk.histogram, cv2.HISTCMP_CORREL)
                if corr < 0.05:
                    continue
            trk.update(detections[d_idx]["bbox"], hist)
            detections[d_idx]["equipment_id"] = f"{det['label']}_{trk.id}"
            if trk.hits < self.min_hits:
                detections[d_idx]["tentative"] = True
            matched_track_set.add(t_idx)
            matched_det_set.add(d_idx)

        # Move unmatched tracks to lost pool before re-ID pass
        still_active = []
        for i, trk in enumerate(self.active_tracks):
            if i in matched_track_set:
                still_active.append(trk)
            elif trk.hits >= self.min_hits:
                self.lost_tracks.append(trk)
        self.active_tracks = still_active

        # Re-ID unmatched detections against lost tracks
        for d_idx in range(len(detections)):
            if d_idx in matched_det_set:
                continue
            det = detections[d_idx]
            hist = histograms[d_idx]

            resurrected = self._try_reid(det["label"], hist)
            if resurrected is not None:
                resurrected.update(det["bbox"], hist)
                self.active_tracks.append(resurrected)
                det["equipment_id"] = f"{det['label']}_{resurrected.id}"
            else:
                trk = Track(det["bbox"], det["label"], hist)
                self.active_tracks.append(trk)
                det["equipment_id"] = f"{det['label']}_{trk.id}"
                det["tentative"] = True

        # Purge expired lost tracks
        self.lost_tracks = [
            t for t in self.lost_tracks if t.time_since_update <= self.max_age
        ]
        return detections

    @staticmethod
    def _compute_histogram(frame, bbox):
        """Compute HSV color histogram of the bounding box region."""
        if frame is None:
            return None
        fh, fw = frame.shape[:2]
        x1, y1 = max(0, int(bbox[0])), max(0, int(bbox[1]))
        x2, y2 = min(fw, int(bbox[2])), min(fh, int(bbox[3]))
        crop = frame[y1:y2, x1:x2]
        if crop.size == 0:
            return None
        hsv = cv2.cvtColor(crop, cv2.COLOR_BGR2HSV)
        hist = cv2.calcHist([hsv], [0, 1], None, [30, 32], [0, 180, 0, 256])
        cv2.normalize(hist, hist)
        return hist

    def _try_reid(self, label, histogram):
        """Search lost tracks for an appearance match (histogram or class fallback)."""
        same_class_lost = [t for t in self.lost_tracks if t.label == label]
        if not same_class_lost:
            return None

        if histogram is not None:
            best, best_score = None, -1
            for trk in same_class_lost:
                if trk.histogram is None:
                    continue
                score = cv2.compareHist(histogram, trk.histogram, cv2.HISTCMP_CORREL)
                if score > self.reid_threshold and score > best_score:
                    best_score = score
                    best = trk
            if best is not None:
                self.lost_tracks.remove(best)
                print(f"    [Re-ID] histogram corr={best_score:.3f} → {label}_{best.id}")
                return best

        # Fallback: reuse the most-seen lost track of the same class
        best = max(same_class_lost, key=lambda t: t.hits)
        self.lost_tracks.remove(best)
        print(f"    [Re-ID] class fallback → {label}_{best.id}")
        return best

    @staticmethod
    def _iou(box_a, box_b):
        """Intersection over Union between two [x1,y1,x2,y2] boxes."""
        x1 = max(box_a[0], box_b[0])
        y1 = max(box_a[1], box_b[1])
        x2 = min(box_a[2], box_b[2])
        y2 = min(box_a[3], box_b[3])
        inter = max(0, x2 - x1) * max(0, y2 - y1)
        area_a = (box_a[2] - box_a[0]) * (box_a[3] - box_a[1])
        area_b = (box_b[2] - box_b[0]) * (box_b[3] - box_b[1])
        union = area_a + area_b - inter
        return inter / union if union > 0 else 0


def create_kafka_producer():
    """Create a Kafka producer with retry logic for container startup ordering."""
    max_retries = 10
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            wait = 3
            print(f"  Kafka not ready (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(wait)
    print(f"WARNING: Kafka not available after {max_retries} retries. Running without streaming.")
    return None


def send_equipment_event(producer, frame_num, fps, detection):
    """Send one equipment event to Kafka."""
    if producer is None:
        return

    event = {
        "frame": frame_num,
        "timestamp_sec": round(frame_num / fps, 2),
        "equipment_id": detection.get("equipment_id", detection["label"]),
        "equipment_label": detection["label"],
        "confidence": detection["confidence"],
        "activity": detection.get("activity", "IDLE"),
        "is_active": detection.get("is_active", False),
        "motion_score": detection.get("motion_score", 0),
        "bbox": detection["bbox"],
        "region_scores": detection.get("region_scores", {}),
    }

    producer.send(KAFKA_TOPIC, value=event)


def save_event_local(events_list, frame_num, fps, detection):
    """Append event to local list (written to JSON at end as backup)."""
    event = {
        "frame": frame_num,
        "timestamp_sec": round(frame_num / fps, 2),
        "equipment_id": detection.get("equipment_id", detection["label"]),
        "equipment_label": detection["label"],
        "confidence": detection["confidence"],
        "activity": detection.get("activity", "IDLE"),
        "is_active": detection.get("is_active", False),
        "motion_score": detection.get("motion_score", 0),
        "bbox": detection["bbox"],
        "region_scores": detection.get("region_scores", {}),
    }
    events_list.append(event)


def load_model():
    """Load the Roboflow client for excavator detection."""
    print(f"Connecting to Roboflow model: {ROBOFLOW_MODEL_ID}...")
    client = InferenceHTTPClient(
        api_url=ROBOFLOW_API_URL,
        api_key=ROBOFLOW_API_KEY,
    )
    print("Roboflow client ready.")
    return client


def detect_equipment(client, frame):
    """Run Roboflow model on a frame; returns list of detections with [x1,y1,x2,y2] bboxes."""
    tmp_path = os.path.join(tempfile.gettempdir(), "cv_frame.jpg")
    cv2.imwrite(tmp_path, frame, [cv2.IMWRITE_JPEG_QUALITY, 80])

    try:
        result = client.infer(tmp_path, model_id=ROBOFLOW_MODEL_ID)
    except Exception as e:
        print(f"  Roboflow API error: {e}")
        return []

    detections = []
    for pred in result.get("predictions", []):
        cx, cy = pred["x"], pred["y"]
        w, h = pred["width"], pred["height"]
        x1 = int(cx - w / 2)
        y1 = int(cy - h / 2)
        x2 = int(cx + w / 2)
        y2 = int(cy + h / 2)

        detections.append({
            "label": pred["class"].lower(),
            "confidence": round(pred["confidence"], 2),
            "bbox": [x1, y1, x2, y2],
        })

    # NMS: remove duplicate overlapping detections from the model
    detections = _nms_per_class(detections, iou_threshold=0.3)

    return detections


def _nms_per_class(detections, iou_threshold=0.3):
    """Non-Maximum Suppression per class: keep highest-confidence box on overlap."""
    if len(detections) <= 1:
        return detections
    # Sort by confidence descending
    detections = sorted(detections, key=lambda d: d["confidence"], reverse=True)
    keep = []
    for det in detections:
        overlap = False
        for kept in keep:
            if kept["label"] != det["label"]:
                continue
            if _iou_boxes(kept["bbox"], det["bbox"]) > iou_threshold:
                overlap = True
                break
        if not overlap:
            keep.append(det)
    return keep


def _iou_boxes(a, b):
    """IoU between two [x1,y1,x2,y2] boxes."""
    x1, y1 = max(a[0], b[0]), max(a[1], b[1])
    x2, y2 = min(a[2], b[2]), min(a[3], b[3])
    inter = max(0, x2 - x1) * max(0, y2 - y1)
    aa = (a[2] - a[0]) * (a[3] - a[1])
    ab = (b[2] - b[0]) * (b[3] - b[1])
    return inter / (aa + ab - inter) if (aa + ab - inter) > 0 else 0


# ---------------------------------------------------------------------------
# Motion Analysis
# ---------------------------------------------------------------------------

def compute_optical_flow(prev_gray, curr_gray):
    """Dense optical flow (Farneback) between two consecutive grayscale frames."""
    flow = cv2.calcOpticalFlowFarneback(
        prev_gray, curr_gray,
        None,
        pyr_scale=0.5,
        levels=3,
        winsize=15,
        iterations=3,
        poly_n=5,
        poly_sigma=1.2,
        flags=0,
    )
    return flow


def analyze_motion_regions(flow, bbox):
    """
    Region-based motion analysis with camera compensation.

    Compares equipment motion against background motion to handle camera shake/pan.
    Splits the bounding box into three sub-regions (top/mid/bot) so that
    articulated motion (e.g., excavator arm moving while tracks are still)
    is correctly detected as ACTIVE.

    Returns: (motion_score, region_scores, region_directions, region_ratios, is_active)
    """
    x1, y1, x2, y2 = bbox
    h = y2 - y1
    w = x2 - x1
    frame_h, frame_w = flow.shape[:2]

    if h <= 0 or w <= 0:
        return 0.0, {}, False

    full_mag = np.sqrt(flow[..., 0] ** 2 + flow[..., 1] ** 2)

    # Background motion (outside bounding box) = camera motion baseline
    bg_mask = np.ones((frame_h, frame_w), dtype=bool)
    # Clamp coordinates
    bx1 = max(0, x1)
    by1 = max(0, y1)
    bx2 = min(frame_w, x2)
    by2 = min(frame_h, y2)
    bg_mask[by1:by2, bx1:bx2] = False

    bg_motion = float(np.mean(full_mag[bg_mask])) if np.any(bg_mask) else 0.0

    # --- Equipment motion inside the bounding box ---
    box_mag = full_mag[by1:by2, bx1:bx2]
    if box_mag.size == 0:
        return 0.0, {}, False

    motion_score = float(np.mean(box_mag))

    # Compensated motion: equipment relative to background
    compensated_motion = motion_score - bg_motion
    motion_ratio = motion_score / bg_motion if bg_motion > 0.01 else (10.0 if motion_score > 0.1 else 0.0)

    # Split into 3 sub-regions for articulated motion detection
    third = max(1, h // 3)
    box_flow = flow[by1:by2, bx1:bx2]

    region_mag_slices = {
        "top": box_mag[0:third, :],
        "mid": box_mag[third:2*third, :],
        "bot": box_mag[2*third:, :],
    }
    region_flow_slices = {
        "top": box_flow[0:third, :],
        "mid": box_flow[third:2*third, :],
        "bot": box_flow[2*third:, :],
    }

    # Background average flow direction (for camera-compensated direction)
    bg_flow_dx = float(np.mean(flow[..., 0][bg_mask])) if np.any(bg_mask) else 0.0
    bg_flow_dy = float(np.mean(flow[..., 1][bg_mask])) if np.any(bg_mask) else 0.0

    region_scores = {}
    region_ratios = {}
    region_directions = {}  # Phase 4: average (dx, dy) per region, camera-compensated
    for name in region_mag_slices:
        region_mag = region_mag_slices[name]
        region_flow = region_flow_slices[name]
        if region_mag.size > 0:
            region_motion = float(np.mean(region_mag))
            region_scores[name] = round(region_motion, 2)
            region_ratios[name] = region_motion / bg_motion if bg_motion > 0.01 else 0.0
            # Camera-compensated flow direction
            avg_dx = float(np.mean(region_flow[..., 0])) - bg_flow_dx
            avg_dy = float(np.mean(region_flow[..., 1])) - bg_flow_dy
            region_directions[name] = (round(avg_dx, 3), round(avg_dy, 3))
        else:
            region_scores[name] = 0.0
            region_ratios[name] = 0.0
            region_directions[name] = (0.0, 0.0)

    # ACTIVE if any sub-region has significant motion above background
    is_active = any(
        region_ratios[name] > MOTION_RATIO_THRESHOLD
        and region_scores[name] > MOTION_MIN_ABSOLUTE
        for name in region_scores
    )

    return round(motion_score, 2), region_scores, region_directions, region_ratios, is_active


def classify_activity(is_active, region_scores, region_directions, region_ratios):
    """
    Classify equipment activity based on optical flow direction per sub-region.

    Uses camera-compensated flow directions:
      - Top region dy > 0 (downward) → DIGGING
      - Top region dy < 0 (upward) → DUMPING
      - Top + mid horizontal motion → SWINGING (turret rotation)
      - Not active → IDLE
    """
    if not is_active:
        return "IDLE"

    top_dx, top_dy = region_directions.get("top", (0, 0))
    mid_dx, mid_dy = region_directions.get("mid", (0, 0))
    top_ratio = region_ratios.get("top", 0)
    mid_ratio = region_ratios.get("mid", 0)

    # Is the top region (arm) moving significantly?
    top_moving = top_ratio > MOTION_RATIO_THRESHOLD
    mid_moving = mid_ratio > SWING_MID_MIN_RATIO

    # Check for SWINGING: both top and mid move horizontally
    if top_moving and mid_moving:
        top_abs_dx = abs(top_dx)
        top_abs_dy = abs(top_dy) + 0.001
        mid_abs_dx = abs(mid_dx)
        mid_abs_dy = abs(mid_dy) + 0.001

        top_is_horizontal = top_abs_dx / top_abs_dy > SWING_HORIZONTAL_RATIO
        mid_is_horizontal = mid_abs_dx / mid_abs_dy > SWING_HORIZONTAL_RATIO

        if top_is_horizontal and mid_is_horizontal:
            return "SWINGING"

    # Check vertical direction of arm (top region)
    if top_moving:
        # Positive dy = downward in image coords
        if top_dy > 0.1:
            return "DIGGING"
        elif top_dy < -0.1:
            return "DUMPING"
        else:
            return "DIGGING"

    return "ACTIVE"


def draw_detections(frame, detections):
    """Draw bounding boxes, sub-region dividers, and motion arrows on the frame."""
    ACTIVITY_COLORS = {
        "DIGGING":  (0, 255, 0),     # Green
        "SWINGING": (0, 255, 255),   # Yellow
        "DUMPING":  (255, 255, 0),   # Cyan
        "IDLE":     (0, 0, 255),     # Red
        "ACTIVE":   (255, 255, 255), # White (fallback)
    }

    for det in detections:
        x1, y1, x2, y2 = det["bbox"]
        activity = det.get("activity", "IDLE")
        motion_score = det.get("motion_score", 0)
        region_scores = det.get("region_scores", {})
        region_directions = det.get("region_directions", {})

        color = ACTIVITY_COLORS.get(activity, (255, 255, 255))

        # Main bounding box
        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

        eq_id = det.get("equipment_id", det["label"])
        label = f"{eq_id} {activity} ({motion_score:.1f})"
        (text_w, text_h), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.6, 1)
        cv2.rectangle(frame, (x1, y1 - text_h - 8), (x1 + text_w, y1), color, -1)
        cv2.putText(frame, label, (x1, y1 - 4),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 0), 1)

        # Sub-region dividers
        h = y2 - y1
        third = h // 3
        for region_name, region_y in [("top", y1 + third), ("mid", y1 + 2 * third)]:
            cv2.line(frame, (x1, region_y), (x2, region_y), color, 1)

        # Per-region scores and direction arrows
        for i, name in enumerate(["top", "mid", "bot"]):
            score = region_scores.get(name, 0)
            ry = y1 + (i * third) + third // 2
            region_color = (0, 255, 0) if score > MOTION_MIN_ABSOLUTE else (100, 100, 100)
            cv2.putText(frame, f"{name}:{score:.2f}", (x2 + 5, ry),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.4, region_color, 1)

            # Motion direction arrow
            dx, dy = region_directions.get(name, (0, 0))
            if abs(dx) > 0.05 or abs(dy) > 0.05:
                arrow_scale = 15  # scale up so arrow is visible
                cx = (x1 + x2) // 2
                cy = y1 + (i * third) + third // 2
                end_x = int(cx + dx * arrow_scale)
                end_y = int(cy + dy * arrow_scale)
                cv2.arrowedLine(frame, (cx, cy), (end_x, end_y), color, 2, tipLength=0.3)

    return frame


def main():
    """Main processing pipeline: detect, track, analyze motion, classify, stream."""
    print(f"=== CV Service Starting ===")

    # --- Open the video ---
    cap = cv2.VideoCapture(VIDEO_SOURCE)
    if not cap.isOpened():
        print(f"ERROR: Cannot open video '{VIDEO_SOURCE}'")
        return

    fps = int(cap.get(cv2.CAP_PROP_FPS))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    print(f"Video: {width}x{height} @ {fps}fps, {total_frames} frames")

    # Output video (unique name per input to avoid overwrites)
    os.makedirs("output", exist_ok=True)
    video_stem = os.path.splitext(os.path.basename(VIDEO_SOURCE))[0]
    output_path = f"output/annotated_{video_stem}.mp4"
    events_path = f"output/events_{video_stem}.json"
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))

    # --- Load model ---
    client = load_model()

    # --- Connect to Kafka ---
    producer = create_kafka_producer()

    # --- Local events list (fallback when no Kafka) ---
    local_events = []

    # Equipment tracker
    tracker = SimpleTracker(
        max_age=200,
        min_hits=3,
        iou_threshold=0.15,
        reid_threshold=0.2,
    )

    # Async detection (overlaps API call with frame processing)
    executor = ThreadPoolExecutor(max_workers=1)
    detection_future = None
    detection_frame = None

    frame_count = 0
    prev_small_gray = None
    cached_detections = []
    last_flow = None
    start_time = time.time()

    small_h = int(height * FLOW_SCALE)
    small_w = int(width * FLOW_SCALE)
    print(f"Optimizations: flow_scale={FLOW_SCALE} ({small_w}x{small_h}), "
          f"flow_every={FLOW_EVERY_N_FRAMES}, detect_every={ROBOFLOW_EVERY_N_FRAMES}, "
          f"async_detect=True")

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        frame_count += 1

        # Downscale for optical flow
        curr_gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        curr_small_gray = cv2.resize(curr_gray, (small_w, small_h))

        # Pick up completed async detection
        if detection_future is not None and detection_future.done():
            try:
                raw_detections = detection_future.result()
                cached_detections = tracker.update(raw_detections, frame=detection_frame)
            except Exception as e:
                print(f"  Detection error: {e}")
            detection_future = None

        # Submit new async detection every N frames
        if frame_count % ROBOFLOW_EVERY_N_FRAMES == 1 and detection_future is None:
            frame_copy = frame.copy()
            detection_future = executor.submit(detect_equipment, client, frame_copy)
            detection_frame = frame_copy

        # Compute optical flow at reduced resolution
        if prev_small_gray is not None and frame_count % FLOW_EVERY_N_FRAMES == 0:
            flow = compute_optical_flow(prev_small_gray, curr_small_gray)
            # Scale flow vectors back to original-resolution magnitude
            last_flow = flow / FLOW_SCALE

        # Analyze motion per detection
        if last_flow is not None and cached_detections:
            for det in cached_detections:
                # Scale bbox to match flow's spatial dimensions
                scaled_bbox = [
                    int(det["bbox"][0] * FLOW_SCALE),
                    int(det["bbox"][1] * FLOW_SCALE),
                    int(det["bbox"][2] * FLOW_SCALE),
                    int(det["bbox"][3] * FLOW_SCALE),
                ]
                motion_score, region_scores, region_directions, region_ratios, is_active = (
                    analyze_motion_regions(last_flow, scaled_bbox)
                )
                det["motion_score"] = motion_score
                det["region_scores"] = region_scores
                det["region_directions"] = region_directions
                det["is_active"] = is_active

                # Excavators get detailed activity; other types just ACTIVE/IDLE
                if "excavator" in det["label"].lower():
                    det["activity"] = classify_activity(
                        is_active, region_scores, region_directions, region_ratios
                    )
                else:
                    det["activity"] = "ACTIVE" if is_active else "IDLE"

        # Send events to Kafka
        if frame_count % KAFKA_SEND_EVERY_N_FRAMES == 0:
            for det in cached_detections:
                if det.get("tentative"):
                    continue
                send_equipment_event(producer, frame_count, fps, det)
                save_event_local(local_events, frame_count, fps, det)

        # Draw and save
        annotated = draw_detections(frame, cached_detections)
        out.write(annotated)

        prev_small_gray = curr_small_gray

        # Progress
        if frame_count % 50 == 0:
            elapsed = time.time() - start_time
            fps_actual = frame_count / elapsed if elapsed > 0 else 0
            pct = (frame_count / total_frames) * 100
            ids = [d.get("equipment_id", "?") for d in cached_detections]
            activities = [d.get("activity", "IDLE") for d in cached_detections]
            info = ", ".join(f"{i}={a}" for i, a in zip(ids, activities))
            print(f"  Frame {frame_count}/{total_frames} ({pct:.0f}%) "
                  f"[{fps_actual:.1f} fps] — {info if info else 'no detections'}")

    # Wait for pending detection
    if detection_future is not None:
        try:
            raw = detection_future.result(timeout=10)
            tracker.update(raw)
        except Exception:
            pass
    executor.shutdown(wait=False)

    # Cleanup
    elapsed = time.time() - start_time
    cap.release()
    out.release()
    if producer is not None:
        producer.flush()
        producer.close()
        print(f"Kafka: all messages flushed and producer closed.")

    # Save events locally (always, as backup / for local mode)
    with open(events_path, "w") as f:
        json.dump(local_events, f, indent=2)
    print(f"Saved {len(local_events)} events to {events_path}")

    print(f"\n=== Done! Processed {frame_count} frames in {elapsed:.1f}s "
          f"({frame_count/elapsed:.1f} fps) ===")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    main()
