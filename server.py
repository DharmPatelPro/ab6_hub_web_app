import asyncio
import json
import logging
import threading
from typing import Set

import websockets
from flask import Flask, send_from_directory
from werkzeug.serving import make_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")

app = Flask(__name__, static_folder='.')

browser_clients: Set[websockets.WebSocketServerProtocol] = set()
interface_clients: Set[websockets.WebSocketServerProtocol] = set()


@app.route('/')
def index():
    response = send_from_directory('.', 'index.html')
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


async def ws_handler(websocket):
    role = None
    try:
        async for message in websocket:
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                continue

            msg_type = payload.get('type')
            role = payload.get('role')

            if role == 'browser':
                if msg_type == 'register':
                    browser_clients.add(websocket)
                    logging.info('Browser connected via websocket')
                    await websocket.send(json.dumps({'type': 'status', 'message': 'connected'}))
                elif msg_type == 'run_code':
                    logging.info('Dispatching run_code to interface client')
                    if not interface_clients:
                        await websocket.send(json.dumps({'type': 'status', 'message': 'no interface connected'}))
                        continue
                    for client in list(interface_clients):
                        try:
                            await client.send(json.dumps({'type': 'run_code', 'data': payload.get('data', {})}))
                        except Exception:
                            interface_clients.discard(client)
                    for client in list(browser_clients):
                        if client is not websocket:
                            try:
                                await client.send(json.dumps({'type': 'status', 'message': 'dispatched to interface'}))
                            except Exception:
                                browser_clients.discard(client)
            elif role == 'interface':
                if msg_type == 'register':
                    interface_clients.add(websocket)
                    logging.info('Interface client connected via websocket')
                    await websocket.send(json.dumps({'type': 'status', 'message': 'interface-ready'}))
                elif msg_type in {'status', 'output', 'finished'}:
                    for client in list(browser_clients):
                        try:
                            await client.send(json.dumps({'type': msg_type, 'message': payload.get('message', ''), 'data': payload.get('data', {})}))
                        except Exception:
                            browser_clients.discard(client)
    except Exception as exc:
        logging.info('Websocket client disconnected: %s', exc)
    finally:
        if role == 'browser':
            browser_clients.discard(websocket)
        elif role == 'interface':
            interface_clients.discard(websocket)


async def start_ws_server():
    async with websockets.serve(ws_handler, '0.0.0.0', 5056):
        await asyncio.Future()


def run_http_server():
    server = make_server('0.0.0.0', 5055, app)
    server.serve_forever()


if __name__ == '__main__':
    logging.info('Starting Web_app HTTP server on http://0.0.0.0:5055')
    logging.info('Starting Web_app WebSocket server on ws://0.0.0.0:5056/ws')

    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    asyncio.get_event_loop().run_until_complete(start_ws_server())
