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

## Setup Steps
1. Clone the repository to your local machine.
2. Install dependencies by running npm install.
3. Ensure you have a Kafka cluster running locally via Docker or a managed service like Confluent Cloud.
4. Create the Kafka topic named location-updates.
5. Create a .env file based on the environment variables section below.
6. Start the main server using node index.js.
7. Start the database processor using node database-processor.js.
8. Open http://localhost:8000 in your browser.

## Environment Variables
The following variables are required in your .env file:
- KAFKA_BROKERS: A comma-separated list of Kafka broker addresses.
- KAFKA_USERNAME: The API key or username for Kafka authentication.
- KAFKA_PASSWORD: The API secret or password for Kafka authentication.
- CLIENT_ID: Your OIDC client identifier.
- CLIENT_SECRET: Your OIDC client secret.
- AUTH_SERVER: The URL of the OIDC identity provider.
- REDIRECT_URI: The callback URL for authentication (usually http://localhost:8000/auth/callback).

## OIDC Auth Setup
The application uses the Proof Key for Code Exchange (PKCE) extension for OAuth 2.0. 
- When a user clicks login, a random code verifier and a hashed code challenge are created.
- The user authenticates on the remote auth server.
- The auth server returns a code to our callback route.
- Our server exchanges this code and the original verifier for an ID token.
- User identity is then stored in a secure http-only cookie.

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
