# Technical Write-Up

## System Overview

This system is a real-time, microservices-based pipeline for monitoring construction equipment utilization from video. It detects equipment (excavators, dump trucks, loaders), determines whether each machine is working or idle, classifies its specific activity, and presents the results on a live dashboard. The pipeline consists of three Python microservices connected through Apache Kafka and backed by TimescaleDB.

The **CV service** processes video frame-by-frame. It runs a Roboflow object detection model to locate equipment, then applies optical flow analysis to determine motion within each bounding box. Detected equipment is tracked across frames using IoU-based matching with HSV histogram re-identification to maintain consistent IDs. Motion and activity results are streamed as JSON events to a Kafka topic.

The **analytics service** consumes those Kafka events, writes them to TimescaleDB, and periodically computes per-equipment utilization summaries (total active time, idle time, utilization percentage, and activity breakdowns). The **dashboard** is a Streamlit web app that reads from TimescaleDB and auto-refreshes to display KPI cards, activity charts, and a live event feed.

## Handling Articulated Motion

A key challenge is that construction equipment has articulated parts. An excavator's arm can be digging while its tracks remain stationary. Treating the whole machine as one region would average out the arm movement, potentially reporting an active machine as idle.

The solution is **region-based motion analysis**. Each equipment bounding box is split vertically into three sub-regions: top (arm/boom), mid (cabin/turret), and bottom (tracks/base). Dense optical flow (Farneback) is computed between consecutive frames, and the average motion magnitude is measured independently in each sub-region. If *any* sub-region shows significant motion relative to the background, the machine is classified as ACTIVE. This ensures that arm-only movement is correctly detected.

**Camera compensation** is also critical. Raw optical flow includes motion from camera pan/shake. To distinguish genuine equipment movement from camera motion, I measure the average motion across the background (all pixels outside bounding boxes) and compare it to the motion inside each sub-region. The equipment is only considered active when its region-to-background motion ratio exceeds a threshold, meaning it moves more than what camera movement alone would explain.

## Activity Classification

Once a machine is determined to be ACTIVE, the system classifies its specific activity using the *direction* of optical flow in each sub-region, after subtracting the background flow direction:

- **Digging**: the top region (arm) has dominant downward motion.
- **Dumping**: the top region has dominant upward motion.
- **Swinging**: both top and mid regions move horizontally (turret rotation), with the horizontal-to-vertical ratio exceeding a threshold.
- **Idle**: the machine is not active.
- **Active**: a catch-all for non-excavator equipment types (e.g., dump trucks) that don't have articulated arm activities.

The direction thresholds are tuned to avoid misclassification from minor camera movements, since the flow values are already camera-compensated.

## Object Tracking and Re-Identification

Maintaining a unique ID per machine across the entire video is handled by a greedy IoU tracker paired with HSV histogram re-identification. Between detection frames, bounding boxes shift minimally (equipment moves slowly relative to frame rate), so IoU matching against last-known positions works reliably. When a track is lost due to a scene cut or temporary occlusion, its HSV color histogram is stored. If a new unmatched detection of the same class appears later with a similar histogram, the original ID is reused rather than spawning a duplicate. A histogram sanity check also guards against false IoU matches across scene cuts by rejecting matches where the appearance changes drastically.

## Design Decisions and Trade-Offs

- **Roboflow API vs. local model**: I chose the Roboflow API for detection because it provides a pre-trained model specifically for construction equipment without requiring local GPU resources. The trade-off is network latency, which I mitigate by running detection asynchronously in a background thread and caching results across frames (detecting every 10th frame).
- **Optical flow at reduced resolution**: Farneback optical flow is computed at 50% scale (4x fewer pixels), cutting computation time significantly while keeping motion estimates accurate enough for activity classification.
- **Greedy vs. optimal matching**: For the small number of objects typically visible in construction footage (1–3), greedy IoU matching produces the same results as globally optimal (Hungarian) assignment, with simpler code and no external dependencies.
- **Kafka as message broker**: Even in this prototype, Kafka decouples the CV pipeline from the analytics backend, allowing each to run and scale independently. The analytics service can be restarted or lag behind without losing events.
- **TimescaleDB over plain PostgreSQL**: TimescaleDB's hypertable optimization makes time-range queries over equipment events efficient out of the box, which is valuable as event volumes grow.

## How to Run and Test

1. Place a construction equipment video at `videos/sample.mp4`.
2. Run `docker compose up --build -d` to start all six containers.
3. Monitor processing with `docker compose logs cv-service -f` — it shows per-frame progress and tracking info.
4. Open `http://localhost:8501` to view the live dashboard with utilization metrics, activity breakdowns, and event timeline.
5. After processing completes, the annotated video is saved at `output/annotated_sample.mp4` with bounding boxes, activity labels, and motion direction arrows overlay.
6. To process a different video: `VIDEO_SOURCE=/videos/other.mp4 docker compose up cv-service -d`.
