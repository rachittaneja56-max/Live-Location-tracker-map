import 'dotenv/config';
import http from 'node:http';
import path from 'node:path';
import crypto from 'node:crypto';

import express from 'express';
import { Server } from 'socket.io';
import cookieParser from 'cookie-parser';

function generateRandomString(length) {
  return crypto.randomBytes(Math.ceil(length / 2)).toString('hex').slice(0, length);
}

function generateCodeChallenge(verifier) {
  return crypto.createHash('sha256').update(verifier).digest('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

import { kafkaClient } from './kafka-client.js';

async function main() {
  const PORT = process.env.PORT ?? 8000;

  const app = express();
  app.use(cookieParser());
  const server = http.createServer(app);
  const io = new Server();

  const kafkaProducer = kafkaClient.producer();
  console.log('Kafka Producer Connecting...');
  await kafkaProducer.connect();
  console.log('Kafka Producer Connected Success');

  const kafkaConsumer = kafkaClient.consumer({
    groupId: `socket-server-${PORT}`,
  });
  console.log('Kafka Consumer Connecting...');
  await kafkaConsumer.connect();
  console.log('Kafka Consumer Connected Success');

  await kafkaConsumer.subscribe({
    topics: ['location-updates'],
    fromBeginning: true,
  });

  kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      try {
        const data = JSON.parse(message.value.toString());
        console.log(`KafkaConsumer: Broadcast update for ${data.name || data.id}`);
        io.emit('server:location:update', {
          id: data.id,
          name: data.name,
          latitude: data.latitude,
          longitude: data.longitude,
        });
        await heartbeat();
      } catch (err) {
        console.error('Error processing Kafka message:', err);
      }
    },
  });

  io.attach(server);

  const parseCookieString = (cookieString) => {
    if (!cookieString) return {};
    return cookieString.split(';').reduce((res, c) => {
      const [key, val] = c.trim().split('=').map(decodeURIComponent);
      try {
        return Object.assign(res, { [key]: JSON.parse(val) });
      } catch (e) {
        return Object.assign(res, { [key]: val });
      }
    }, {});
  };

  io.use((socket, next) => {
    const cookies = parseCookieString(socket.request.headers.cookie);
    if (cookies.auth_user) {
      socket.user = cookies.auth_user;
      next();
    } else {
      next(new Error('Authentication required'));
    }
  });

  io.on('connection', (socket) => {
    console.log(`[Socket:${socket.id}]: Connected (User: ${socket.user.name})`);

    socket.on('client:location:update', async (locationData) => {
      const { latitude, longitude } = locationData;
      const user = socket.user;
      console.log(`[Socket:${socket.id}]: Received location update from ${user.name}`);

      try {
        await kafkaProducer.send({
          topic: 'location-updates',
          messages: [
            {
              key: user.id,
              value: JSON.stringify({ id: user.id, name: user.name, latitude, longitude }),
            },
          ],
        });
      } catch (err) {
        console.error('Error producing to Kafka:', err);
      }
    });

    socket.on('disconnect', () => {
      console.log(`[Socket:${socket.id}]: Disconnected`);
      io.emit('server:user:disconnected', { id: socket.user.id });
    });
  });

  app.use(express.static(path.resolve('./public')));

  const AUTH_SERVER = process.env.AUTH_SERVER;
  const CLIENT_ID = process.env.CLIENT_ID;
  const CLIENT_SECRET = process.env.CLIENT_SECRET;
  const REDIRECT_URI = process.env.REDIRECT_URI;

  app.get('/auth/login', (req, res) => {
    const codeVerifier = generateRandomString(43);
    const codeChallenge = generateCodeChallenge(codeVerifier);

    res.cookie('pkce_verifier', codeVerifier, { httpOnly: true, secure: false, maxAge: 5 * 60 * 1000 });

    const authUrl = new URL(`${AUTH_SERVER}/authorize`);
    authUrl.searchParams.append('client_id', CLIENT_ID);
    authUrl.searchParams.append('redirect_uri', REDIRECT_URI);
    authUrl.searchParams.append('response_type', 'code');
    authUrl.searchParams.append('code_challenge', codeChallenge);
    authUrl.searchParams.append('code_challenge_method', 'S256');

    res.redirect(authUrl.toString());
  });

  app.get('/auth/callback', async (req, res) => {
    const code = req.query.code;
    const codeVerifier = req.cookies.pkce_verifier;

    if (!code || !codeVerifier) {
      return res.status(400).send('Missing code or verifier');
    }

    try {
      const response = await fetch(`${AUTH_SERVER}/api/auth/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          client_id: CLIENT_ID,
          client_secret: CLIENT_SECRET,
          grant_type: 'authorization_code',
          code,
          redirect_uri: REDIRECT_URI,
          code_verifier: codeVerifier,
        }),
      });

      const data = await response.json();
      if (!response.ok) throw new Error(data.message || 'Token exchange failed');

      const idTokenParts = data.id_token.split('.');
      const payload = JSON.parse(Buffer.from(idTokenParts[1], 'base64').toString());

      res.clearCookie('pkce_verifier');
      res.cookie('auth_user', JSON.stringify({
        id: payload.sub || payload.email || generateRandomString(10),
        name: payload.name || payload.email || 'Unknown User',
        email: payload.email,
      }), { httpOnly: true, secure: false, maxAge: 24 * 60 * 60 * 1000 });

      res.redirect('/');
    } catch (err) {
      console.error('Auth callback error:', err);
      res.status(500).send('Authentication failed');
    }
  });

  app.get('/auth/logout', (req, res) => {
    // Clear local session
    res.clearCookie('auth_user');
    
    // Redirect immediately back to the app's home page
    // (Bypasses global SSO logout to ensure user stays on the app)
    res.redirect('/');
  });

  app.get('/auth/me', (req, res) => {
    const authCookie = req.cookies.auth_user;
    if (authCookie) {
      res.json(JSON.parse(authCookie));
    } else {
      res.status(401).json({ error: 'Not authenticated' });
    }
  });

  app.get('/health', (req, res) => {
    return res.json({ healthy: true });
  });

  server.listen(PORT, () =>
    console.log(`Server running on http://localhost:${PORT}`),
  );

  // Graceful shutdown
  const shutdown = async () => {
    console.log('\nShutting down...');
    await kafkaProducer.disconnect();
    await kafkaConsumer.disconnect();
    server.close(() => {
      console.log('Server closed');
      process.exit(0);
    });
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
