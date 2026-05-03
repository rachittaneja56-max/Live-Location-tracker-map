# Real-Time Location Tracker with Apache Kafka

## Project Overview
This project is a real-time location sharing application. It allows users to log in securely, share their live coordinates, and view the movement of all other active users on a map. The system is designed to handle high-frequency updates by using Apache Kafka as the central message broker.

## Tech Stack
- Frontend: HTML5, Vanilla JavaScript, Leaflet.js for maps, Tailwind CSS for styling.
- Backend: Node.js, Express.js.
- Real-Time Communication: Socket.IO.
- Message Broker: Apache Kafka (using KafkaJS).
- Authentication: OIDC / OAuth 2.0 with PKCE flow.
- Environment Management: Dotenv.

## How to Run This Project Locally
If you are cloning this repository, follow these detailed steps to set up the environment on your machine.

### 1. Prerequisites
- [Node.js](https://nodejs.org/) installed (v18 or higher recommended).
- [Docker](https://www.docker.com/) installed (to run the local Kafka cluster).

### 2. Setup Steps
1. **Clone the repository:**
   ```bash
   git clone https://github.com/rachittaneja56-max/Live-Location-tracker-map.git
   cd Live-Location-tracker-map
   ```
2. **Install dependencies:**
   ```bash
   npm install
   ```
3. **Start the local Kafka cluster:**
   This repository includes a `docker-compose.yml` file to instantly spin up Apache Kafka locally.
   ```bash
   docker-compose up -d
   ```
4. **Create the Kafka topic:**
   We have included an admin script to automatically create the required `location-updates` topic.
   ```bash
   node kafka-admin.js
   ```

### 3. Environment & Authentication Setup
This application requires an OIDC-compliant Identity Provider (like Rachit's Auth, Auth0, or Keycloak) to handle user logins. You cannot use the default credentials; you must generate your own.

1. **Create your `.env` file:**
   Rename `.env.example` to `.env`.
   
2. **Register your application:**
   - Go to your Identity Provider (e.g., `https://auth.rachittaneja.in`).
   - Register a new "Client Application".
   - Set your **Redirect URI** to: `http://localhost:8000/auth/callback`.
   
3. **Fill in the `.env` file:**
   - Paste your new `CLIENT_ID` and `CLIENT_SECRET` into the `.env` file.
   - Leave `KAFKA_USERNAME` and `KAFKA_PASSWORD` entirely blank (these are only needed if you deploy to a managed cloud Kafka later).

### 4. Run the Application
1. **Start the main server:**
   ```bash
   npm start
   ```
2. **Start the database processor:**
   Open a *second* terminal window and run:
   ```bash
   node database-processor.js
   ```
3. **Open the app:**
   Visit `http://localhost:8000` in your web browser. Open it in multiple tabs/devices to see the real-time movement in action!

## Socket Event Flow
- connection: Triggered when a browser opens the app. Middleware validates the auth cookie.
- client:location:update: Sent by the browser every 10 seconds with latitude and longitude.
- server:location:update: Sent by the server to all browsers when a Kafka event is consumed.
- server:user:disconnected: Sent to all browsers when a user closes their tab to remove their marker.

## Kafka Event Flow
1. Producer: The socket server receives a location update and publishes it to the location-updates topic. The user ID is used as the message key.
2. Consumer Group 1 (Sockets): This group reads messages and broadcasts them to active web sockets for live map updates.
3. Consumer Group 2 (Database): This group reads the same messages independently to simulate storing them in a persistent database.

## Demo Video Link
[Insert your recorded demo video URL here]

## Assumptions and Limitations
- The application assumes the browser has a working internet connection and GPS/Geolocation permissions are granted.
- The free tier of the map provider may have tile loading limits.
- The current implementation uses a simulated database log instead of a physical database connection.
- Authentication depends on the availability of the configured OIDC provider.
- Location updates are sent every 10 seconds to balance accuracy and network usage.
