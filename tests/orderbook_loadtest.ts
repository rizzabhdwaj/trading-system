import http from 'k6/http';
import { check } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 10 },
    { duration: '20s', target: 25 },
    { duration: '20s', target: 50 },
    { duration: '20s', target: 100 },
    { duration: '10s', target: 0 },
  ],

  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1800'],
  },
};

// const BASE_URL = 'http://localhost:8080';
const BASE_URL="http://127.0.0.1:8080"

const creds: { username: string; password: string; role: string }[] =
  JSON.parse(open('./loadtest_users2.json'));

interface ActiveUser {
  username: string;
  role: string;
  token: string;
}

export function setup(): { users: ActiveUser[] } {
  const users: ActiveUser[] = [];

  for (const c of creds) {
    const res = http.post(`${BASE_URL}/api/login`, JSON.stringify({
      username: c.username,
      password: c.password,
    }), {
      headers: { 'Content-Type': 'application/json' },
    });

    if (res.status === 200) {
      const body = JSON.parse(res.body as string);
      users.push({ username: c.username, role: c.role, token: body.token });
    }
  }

  console.log(`Logged in ${users.length}/${creds.length} users`);
  return { users };
}

export default function(data: { users: ActiveUser[] }) {
  if (!data.users || data.users.length === 0) return;

  const user = data.users[Math.floor(Math.random() * data.users.length)];

  const headers = {
    Authorization: `Bearer ${user.token}`,
    'Content-Type': 'application/json',
  };

  const side = user.role === 'seller' ? 'SELL' : 'BUY';
  const price = Math.floor(Math.random() * 5) + 98;
  const qty = Math.floor(Math.random() * 5) + 1;

  const orderRes = http.post(`${BASE_URL}/api/order`, JSON.stringify({
    side,
    qty,
    price,
  }), { headers });
  if (orderRes.status !== 200) {
    console.error(
      `status=${orderRes.status} error=${orderRes.error} user=${user.username} side=${side} qty=${qty} price=${price} body=${orderRes.body}`
    );
  }
  check(orderRes, {
    'order status is 200': (r) => r.status === 200,
  });
}
