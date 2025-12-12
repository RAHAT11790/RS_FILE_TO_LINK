from Adarsh.vars import Var
from Adarsh.bot import StreamBot
from Adarsh.utils.human_readable import humanbytes
from Adarsh.utils.file_properties import get_file_ids
from Adarsh.server.exceptions import InvalidHash
import urllib.parse
import aiofiles
import logging
import aiohttp


async def render_page(id, secure_hash):

    # Get file details
    file_data = await get_file_ids(StreamBot, int(Var.BIN_CHANNEL), int(id))

    # Unique hash validation FIXED
    true_hash = file_data.unique_id[:6]
    if true_hash != secure_hash:
        logging.debug(f"Invalid hash => got:{secure_hash} expected:{true_hash}")
        raise InvalidHash

    # Build final file URL
    src = urllib.parse.urljoin(Var.URL, f"{secure_hash}{id}")

    # Load your new full HTML template
    async with aiofiles.open("Adarsh/template/player.html", mode="r", encoding="utf-8") as f:
        html = await f.read()

    # Video / Audio / Other type handling
    file_type = file_data.mime_type.split("/")[0]

    # For non-video files → get file size
    if file_type not in ["video", "audio"]:
        async with aiohttp.ClientSession() as s:
            async with s.get(src) as u:
                file_size = humanbytes(int(u.headers.get("Content-Length", 0)))
        html = html.replace("{{FILE_SIZE}}", file_size)
    else:
        html = html.replace("{{FILE_SIZE}}", "")

    # Replace placeholders in HTML
    html = (
        html.replace("{{VIDEO_SRC}}", src)
            .replace("{{FILE_NAME}}", file_data.file_name)
            .replace("{{PAGE_TITLE}}", f"Watch {file_data.file_name}")
    )

    return html
