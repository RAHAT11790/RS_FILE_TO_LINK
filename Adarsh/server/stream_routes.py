# Taken from megadlbot_oss
# Optimized for Render + Auto-Repair Streaming

import re
import time
import math
import logging
import secrets
import mimetypes
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine

from Adarsh.bot import multi_clients, work_loads, StreamBot
from Adarsh.server.exceptions import FIleNotFound, InvalidHash
from Adarsh import StartTime, __version__
from ..utils.time_format import get_readable_time
from ..utils.custom_dl import ByteStreamer
from Adarsh.utils.render_template import render_page
from Adarsh.vars import Var


routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(_):
    return web.json_response(
        {
            "server_status": "running",
            "uptime": get_readable_time(time.time() - StartTime),
            "telegram_bot": "@" + StreamBot.username,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "version": __version__,
        }
    )


@routes.get(r"/watch/{path:\S+}", allow_head=True)
async def watch_page(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            msg_id = int(match.group(2))
        else:
            msg_id = int(re.search(r"(\d+)", path).group(1))
            secure_hash = request.rel_url.query.get("hash")

        return web.Response(
            text=await render_page(msg_id, secure_hash),
            content_type="text/html",
        )

    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        logging.exception(e)
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r"/{path:\S+}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            msg_id = int(match.group(2))
        else:
            msg_id = int(re.search(r"(\d+)", path).group(1))
            secure_hash = request.rel_url.query.get("hash")

        return await media_streamer(request, msg_id, secure_hash)

    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        logging.exception(e)
        raise web.HTTPInternalServerError(text=str(e))


class_cache = {}

async def media_streamer(request: web.Request, msg_id: int, secure_hash: str):
    range_header = request.headers.get("Range")

    index = min(work_loads, key=work_loads.get)
    client = multi_clients[index]

    if client not in class_cache:
        class_cache[client] = ByteStreamer(client)

    streamer = class_cache[client]
    file_id = await streamer.get_file_properties(msg_id)

    if file_id.unique_id[:6] != secure_hash:
        raise InvalidHash

    file_size = file_id.file_size

    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = (request.http_range.stop or file_size) - 1

    if until_bytes >= file_size:
        until_bytes = file_size - 1

    # ✅ OPTIMIZED CHUNK SIZE (BEST FOR MULTI USERS)
    chunk_size = 512 * 1024

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)

    body = streamer.yield_file(
        file_id,
        index,
        offset,
        first_part_cut,
        last_part_cut,
        part_count,
        chunk_size,
    )

    mime_type = file_id.mime_type or "application/octet-stream"
    file_name = file_id.file_name or f"{secrets.token_hex(2)}.bin"

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers={
            "Content-Type": mime_type,
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f'inline; filename="{file_name}"',
            "Accept-Ranges": "bytes",
        },
    )
