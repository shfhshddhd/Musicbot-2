# Copyright (c) 2025 TheHamkerAlone
# Licensed under the MIT License.
# This file is part of AloneXMusic
# ALONE-CODER

from ntgcalls import (ConnectionNotFound, TelegramServerError,
                      RTMPStreamingUnsupported)
from pyrogram.errors import MessageIdInvalid
from pyrogram.types import InputMediaPhoto, Message
from pytgcalls import PyTgCalls, exceptions, types
from pytgcalls.pytgcalls_session import PyTgCallsSession

from AloneX import app, config, db, lang, logger, queue, userbot, yt
from AloneX.helpers import Media, Track, buttons, thumb
from AloneX.helpers.downloads import download_track
from AloneX.helpers.autoplay_ui import controls_with_autoplay
from AloneX.helpers.autoplay import candidates as autoplay_candidates


class TgCall(PyTgCalls):
    def __init__(self):
        self.clients = []
        self._transitioning = set()
        self._prefetch_tasks = {}

    async def pause(self, chat_id: int) -> bool:
        client = await db.get_assistant(chat_id)
        await db.playing(chat_id, paused=True)
        return await client.pause(chat_id)

    async def resume(self, chat_id: int) -> bool:
        client = await db.get_assistant(chat_id)
        await db.playing(chat_id, paused=False)
        return await client.resume(chat_id)

    async def stop(self, chat_id: int) -> None:
        client = await db.get_assistant(chat_id)
        current = queue.get_current(chat_id)

        # Remove the active song message whenever playback is stopped,
        # whether stop came from the button, /stop, or an internal error.
        if current and current.message_id:
            try:
                await app.delete_messages(
                    chat_id=chat_id,
                    message_ids=current.message_id,
                    revoke=True,
                )
            except Exception:
                pass
            current.message_id = 0

        try:
            queue.clear(chat_id)
            await db.remove_call(chat_id)
        except:
            pass

        try:
            await client.leave_call(chat_id, close=False)
        except:
            pass


    async def play_media(
        self,
        chat_id: int,
        message: Message,
        media: Media | Track,
        seek_time: int = 0,
    ) -> None:
        client = await db.get_assistant(chat_id)
        _lang = await lang.get_lang(chat_id)
        media.message_id = message.id
        _thumb = (
            await thumb.generate(media)
            if isinstance(media, Track)
            else config.DEFAULT_THUMB
        )

        if not media.file_path:
            await message.edit_text(_lang["error_no_file"].format(config.SUPPORT_CHAT))
            return await self.play_next(chat_id)

        stream = types.MediaStream(
            media_path=media.file_path,
            audio_parameters=types.AudioQuality.HIGH,
            video_parameters=types.VideoQuality.HD_720p,
            audio_flags=types.MediaStream.Flags.REQUIRED,
            video_flags=(
                types.MediaStream.Flags.AUTO_DETECT
                if media.video
                else types.MediaStream.Flags.IGNORE
            ),
            ffmpeg_parameters=f"-ss {seek_time}" if seek_time > 1 else None,
        )
        try:
            await db.add_autoplay_history(chat_id, media.id)
            await client.play(
                chat_id=chat_id,
                stream=stream,
                config=types.GroupCallConfig(auto_start=False),
            )
            if not seek_time:
                media.time = 1
                await db.add_call(chat_id)
                self._schedule_next_prefetch(chat_id, media.id)
                text = _lang["play_media"].format(
                    media.url,
                    media.title,
                    media.duration,
                    media.user,
                )
                keyboard = await controls_with_autoplay(chat_id)
                try:
                    await message.edit_media(
                        media=InputMediaPhoto(
                            media=_thumb,
                            caption=text,
                        ),
                        reply_markup=keyboard,
                    )
                except MessageIdInvalid:
                    media.message_id = (await app.send_photo(
                        chat_id=chat_id,
                        photo=_thumb,
                        caption=text,
                        reply_markup=keyboard,
                    )).id
        except FileNotFoundError:
            await message.edit_text(_lang["error_no_file"].format(config.SUPPORT_CHAT))
            await self.play_next(chat_id)
        except exceptions.NoActiveGroupCall:
            await self.stop(chat_id)
            await message.edit_text(_lang["error_no_call"])
        except exceptions.NoAudioSourceFound:
            await message.edit_text(_lang["error_no_audio"])
            await self.play_next(chat_id)
        except (ConnectionNotFound, TelegramServerError):
            await self.stop(chat_id)
            await message.edit_text(_lang["error_tg_server"])
        except RTMPStreamingUnsupported:
            await self.stop(chat_id)
            await message.edit_text(_lang["error_rtmp"])



    def _schedule_next_prefetch(self, chat_id: int, current_id: str) -> None:
        """Download the next queued item while the current track is playing."""
        import asyncio

        next_media = queue.get_next(chat_id, check=True)
        if not next_media or next_media.id == current_id or next_media.file_path:
            return
        key = (chat_id, str(next_media.id), bool(next_media.video))
        existing = self._prefetch_tasks.get(key)
        if existing and not existing.done():
            return

        async def prefetch():
            try:
                path = await download_track(next_media)
                if path:
                    logger.info("[prefetch] ready: %s (%s)", next_media.title, next_media.id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[prefetch] failed for %s", next_media.id)
            finally:
                self._prefetch_tasks.pop(key, None)

        self._prefetch_tasks[key] = asyncio.create_task(prefetch())

    async def replay(self, chat_id: int) -> None:
        if not await db.get_call(chat_id):
            return

        media = queue.get_current(chat_id)
        _lang = await lang.get_lang(chat_id)
        msg = await app.send_message(chat_id=chat_id, text=_lang["play_again"])
        await self.play_media(chat_id, msg, media)


    async def _get_autoplay_track(
        self, chat_id: int, last_media: Media | Track, message_id: int
    ):
        if not last_media or not await db.get_autoplay(chat_id):
            return None

        history = await db.get_autoplay_history(chat_id)

        try:
            recommendations = await autoplay_candidates(
                last_media if isinstance(last_media, Track) else Track(
                    id=last_media.id,
                    channel_name="",
                    duration=last_media.duration,
                    duration_sec=last_media.duration_sec,
                    title=last_media.title,
                    url=last_media.url,
                    video=last_media.video,
                ),
                limit=10,
            )
        except Exception as ex:
            logger.error(f"[autoplay] recommendation engine failed in {chat_id}: {ex}")
            recommendations = []

        for candidate in recommendations:
            if not candidate or not candidate.id:
                continue
            if candidate.id == last_media.id or candidate.id in history:
                continue
            candidate.user = "♫ Autoplay"
            await db.add_autoplay_history(chat_id, candidate.id)
            logger.info(
                f"[autoplay] selected recommendation {candidate.title} ({candidate.id})"
            )
            return candidate

        # Same fallback idea as the reference: search related text when
        # Mix/related recommendations are unavailable.
        queries = [
            last_media.title,
            f"{last_media.title} song",
        ]
        channel = getattr(last_media, "channel_name", "")
        if channel:
            queries.insert(1, f"{last_media.title} {channel}")

        for query in queries:
            if not query:
                continue
            try:
                candidate = await yt.search(
                    query,
                    message_id,
                    video=getattr(last_media, "video", False),
                )
            except Exception as ex:
                logger.error(f"[autoplay] search fallback failed in {chat_id}: {ex}")
                continue

            if not candidate or candidate.id == last_media.id or candidate.id in history:
                continue

            candidate.user = "♫ Autoplay"
            await db.add_autoplay_history(chat_id, candidate.id)
            logger.info(
                f"[autoplay] selected fallback {candidate.title} ({candidate.id})"
            )
            return candidate

        return None

    async def play_next(
        self, chat_id: int, expected_message_id: int | None = None
    ) -> bool:
        # Only one transition may run at a time. This prevents double-taps on
        # Skip (and simultaneous StreamEnded/Skip events) from skipping twice.
        if chat_id in self._transitioning:
            return False
        self._transitioning.add(chat_id)

        try:
            last_media = queue.get_current(chat_id)

            if expected_message_id is not None:
                if not last_media or last_media.message_id != expected_message_id:
                    return False

            media = queue.get_next(chat_id)

            # The message belongs to the song that just finished/skipped.
            # Delete that old song message before showing the next song.
            if last_media and last_media.message_id:
                try:
                    await app.delete_messages(
                        chat_id=chat_id,
                        message_ids=last_media.message_id,
                        revoke=True,
                    )
                except Exception:
                    pass
                last_media.message_id = 0

            finding_msg = None
            if not media and last_media and await db.get_autoplay(chat_id):
                finding_msg = await app.send_message(
                    chat_id=chat_id,
                    text="♫ Finding next song...",
                )
                auto_media = await self._get_autoplay_track(
                    chat_id, last_media, finding_msg.id
                )

                if auto_media:
                    queue.force_add(chat_id, auto_media)
                    media = queue.get_current(chat_id)
                else:
                    try:
                        await finding_msg.delete()
                    except Exception:
                        pass
                    return await self.stop(chat_id) or False

            if not media:
                return await self.stop(chat_id) or False

            # Do not leave the temporary "Finding next song..." message
            # behind once the next song has been selected.
            if finding_msg:
                try:
                    await finding_msg.delete()
                except Exception:
                    pass

            _lang = await lang.get_lang(chat_id)
            msg = await app.send_message(chat_id=chat_id, text=_lang["play_next"])
            if not media.file_path:
                media.file_path = await download_track(media)
                if not media.file_path:
                    await self.stop(chat_id)
                    try:
                        await msg.delete()
                    except Exception:
                        pass
                    return False

            media.message_id = msg.id
            await self.play_media(chat_id, msg, media)
            return True
        finally:
            self._transitioning.discard(chat_id)


    async def ping(self) -> float:
        pings = [client.ping for client in self.clients]
        return round(sum(pings) / len(pings), 2)


    async def decorators(self, client: PyTgCalls) -> None:
        @client.on_update()
        async def update_handler(_, update: types.Update) -> None:
            if isinstance(update, types.StreamEnded):
                if update.stream_type == types.StreamEnded.Type.AUDIO:
                    await self.play_next(update.chat_id)
            elif isinstance(update, types.ChatUpdate):
                if update.status in [
                    types.ChatUpdate.Status.KICKED,
                    types.ChatUpdate.Status.LEFT_GROUP,
                    types.ChatUpdate.Status.CLOSED_VOICE_CHAT,
                ]:
                    await self.stop(update.chat_id)


    async def boot(self) -> None:
        PyTgCallsSession.notice_displayed = True
        for ub in userbot.clients:
            client = PyTgCalls(ub, cache_duration=100)
            await client.start()
            self.clients.append(client)
            await self.decorators(client)
        logger.info("PyTgCalls client(s) started.")
