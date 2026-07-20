#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2026 tmwgsicp
# Licensed under the GNU Affero General Public License v3.0
# See LICENSE file in the project root for full license text.
# SPDX-License-Identifier: AGPL-3.0-only
"""
导出下载路径的「服务端取图器 + PDF 渲染」。

阅读路径（RSS/网页）用图片代理直连 mmbiz，最快；
下载路径（PDF/Word/EPUB）必须**自包含、离线可读** → 不能引用 /api/image，必须把图片内嵌进文档：

    服务端取图器带合法 referer(mp.weixin.qq.com) 抓原图 → Pillow 压缩(限边长+JPEG) → base64 内嵌

- PDF：xhtml2pdf(pisa) + reportlab 内置 CJK 字体 STSong-Light（免装字体文件即可渲染中文）。
- Word/EPUB：见 docx_render / epub_render，复用这里的取图器。

单机自托管版：不接 Redis（无缓存层），靠单次导出内的「唯一图去重」避免重复抓取；
全局并发闸限制同时抓图数，避免多个导出同时把服务器和 mmbiz 压垮。
限额：单次文章数由 export.py 卡；此处再兜底「图片张数 / 总字节」，防单个巨图号把文档撑爆内存。
"""
import asyncio
import base64
import io
import logging
import re
from urllib.parse import unquote

import httpx

logger = logging.getLogger(__name__)

_WX_HOSTS = ("mmbiz.qpic.cn", "mmbiz.qlogo.cn", "wx.qlogo.cn", "res.wx.qq.com")

# 全局跨「所有导出请求」共享的抓图并发闸：单请求提速，同时多个导出并跑也不会叠加把服务器
# 和 mmbiz 压垮（共用这些并发，不是各占各的）。绑事件循环，单进程 async 下即全局。
_GLOBAL_IMG_SEM = asyncio.Semaphore(16)

_IMG_TAG_RE = re.compile(r'<img\b[^>]*>', re.IGNORECASE)
_SRC_RE = re.compile(r'\bsrc="([^"]+)"', re.IGNORECASE)


def _real_url(src: str) -> str:
    """代理 URL(<host>/api/image?url=X) → 还原直连 X；直连原样返回。"""
    m = re.search(r'/api/image\?url=(.+)$', src)
    if m:
        return unquote(m.group(1))
    return src


def _is_wx_image(url: str) -> bool:
    return url.startswith("http") and any(h in url for h in _WX_HOSTS)


def _compress_to_datauri(raw: bytes, max_side: int, jpeg_q: int) -> str:
    """Pillow 压缩：转 RGB(白底展平透明) + 限最长边 + JPEG。失败则原图内嵌兜底。
    已够小的图(尺寸够 + 字节小 + jpeg/png)直接原样内嵌，跳过昂贵重编码——微信图多数本就 web 优化过。"""
    try:
        from PIL import Image
        im = Image.open(io.BytesIO(raw))
        im.load()
        w, h = im.size
        fmt = (im.format or "").lower()
        # 快路径：尺寸已够小 + 字节不大 + 本就是 jpeg/png → 原样内嵌，省掉解码后重编码的 CPU
        if max(w, h) <= max_side and len(raw) <= 220 * 1024 and fmt in ("jpeg", "jpg", "png"):
            mime = "image/png" if fmt == "png" else "image/jpeg"
            return f"data:{mime};base64," + base64.b64encode(raw).decode()
        if im.mode in ("RGBA", "LA", "P"):
            im = im.convert("RGBA")
            bg = Image.new("RGB", im.size, (255, 255, 255))
            bg.paste(im, mask=im.split()[-1])
            im = bg
        else:
            im = im.convert("RGB")
        if max(w, h) > max_side:
            if w >= h:
                im = im.resize((max_side, max(1, round(h * max_side / w))))
            else:
                im = im.resize((max(1, round(w * max_side / h)), max_side))
        out = io.BytesIO()
        im.save(out, format="JPEG", quality=jpeg_q)  # 不加 optimize：显著更快，体积仅略大
        return "data:image/jpeg;base64," + base64.b64encode(out.getvalue()).decode()
    except Exception:
        return "data:image/jpeg;base64," + base64.b64encode(raw).decode()


async def _fetch_datauri(client: httpx.AsyncClient, url: str, max_side: int, jpeg_q: int):
    async with _GLOBAL_IMG_SEM:  # 全局限并发，多导出共享
        try:
            r = await client.get(url, headers={"Referer": "https://mp.weixin.qq.com/"},
                                 timeout=12.0, follow_redirects=True)
            if r.status_code != 200 or not r.content:
                return None
            raw = r.content
        except Exception:
            return None
    # 压缩放线程池，避免大图阻塞事件循环
    return await asyncio.to_thread(_compress_to_datauri, raw, max_side, jpeg_q)


def _datauri_to_bytes(data_uri: str):
    """data:...;base64,XXXX → 原始 bytes（给 docx add_picture 用）。"""
    try:
        return base64.b64decode(data_uri.split(",", 1)[1])
    except Exception:
        return None


async def fetch_images_bytes(urls: list, max_side: int, jpeg_q: int = 80) -> dict:
    """并发抓+压一批微信图，返回 {真实url: 压缩后bytes}。复用全局并发闸。
    给真 .docx/EPUB 用（按图哈希去重、各存一份）。"""
    out = {}
    urls = [u for u in dict.fromkeys(urls) if _is_wx_image(u)]  # 去重保序 + 只留微信图
    if not urls:
        return out
    async with httpx.AsyncClient() as client:
        res = await asyncio.gather(*[_fetch_datauri(client, u, max_side, jpeg_q) for u in urls])
    for u, du in zip(urls, res):
        if du:
            b = _datauri_to_bytes(du)
            if b:
                out[u] = b
    return out


async def inline_images(html: str, *, max_images: int = 800, max_side: int = 900,
                        jpeg_q: int = 80, max_total_bytes: int = 55 * 1024 * 1024) -> str:
    """
    把正文里的微信图 <img src> 全部换成内嵌 data-URI（离线自包含）。
    未内嵌（超张数/超总字节/抓取失败/非微信图）的 <img> 直接删除，避免离线破图占位。
    """
    if not html:
        return html
    # 收集唯一原图 URL（保持出现顺序，前 max_images 张优先）
    uniq, seen = [], set()
    for tag in _IMG_TAG_RE.findall(html):
        m = _SRC_RE.search(tag)
        if not m:
            continue
        real = _real_url(m.group(1))
        if _is_wx_image(real) and real not in seen:
            seen.add(real)
            uniq.append(real)
    uniq = uniq[:max_images]

    mapping = {}
    if uniq:
        async with httpx.AsyncClient() as client:
            results = await asyncio.gather(
                *[_fetch_datauri(client, u, max_side, jpeg_q) for u in uniq]
            )
        total = 0
        for u, data_uri in zip(uniq, results):
            if not data_uri:
                continue
            total += len(data_uri)
            if total > max_total_bytes:
                break
            mapping[u] = data_uri
    logger.info("export inline_images: %d 张唯一图, 成功内嵌 %d", len(uniq), len(mapping))

    def _repl(m):
        tag = m.group(0)
        sm = _SRC_RE.search(tag)
        if not sm:
            return ""
        du = mapping.get(_real_url(sm.group(1)))
        if du:
            return tag.replace(sm.group(1), du)
        return ""  # 未内嵌 → 删除，避免离线破图

    return _IMG_TAG_RE.sub(_repl, html)


# ---------- PDF ----------

_CJK_READY = False


def _ensure_cjk_font():
    global _CJK_READY
    if _CJK_READY:
        return
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.cidfonts import UnicodeCIDFont
    pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))  # reportlab 内置 Adobe 简中，免字体文件
    _CJK_READY = True


def render_pdf(self_contained_html: str) -> bytes:
    """自包含 HTML(图片已 data-URI 内嵌) → PDF bytes。同步 CPU 活，调用方请放线程池。"""
    _ensure_cjk_font()
    from xhtml2pdf import pisa
    buf = io.BytesIO()
    pisa.CreatePDF(src=self_contained_html, dest=buf, encoding="utf-8")
    return buf.getvalue()
