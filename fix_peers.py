import asyncio
import os
from dotenv import load_dotenv
from pyrogram import Client

load_dotenv()

userbot = Client(
    "session",
    api_id=int(os.getenv("API_ID", "0")),
    api_hash=os.getenv("API_HASH", ""),
    phone_number=os.getenv("PHONE", "")
)

CHANNELS = {}
for env_key in ["CHANNEL_1", "CHANNEL_2", "CHANNEL_3", "CHANNEL_4",
                 "CHANNEL_5", "CHANNEL_6", "CHANNEL_7",
                 "IMBA_GROUP_ID", "BFS_GROUP_ID"]:
    val = os.getenv(env_key)
    if val:
        CHANNELS[env_key] = val


async def main():
    await userbot.start()
    print("=== Pyrogram connected ===\n")

    # Step 1: Load all dialogs into cache
    print("Loading dialogs into peer cache...")
    count = 0
    async for dialog in userbot.get_dialogs():
        chat = dialog.chat
        title = chat.title or chat.first_name or chat.username or "N/A"
        print(f"  {chat.id:>15} | {chat.type.name:<12} | {title}")
        count += 1
    print(f"\nTotal dialogs loaded: {count}\n")

    # Step 2: Try to resolve each channel
    print("=" * 60)
    print("Resolving channels from .env:")
    print("=" * 60)

    ok = []
    fail = []
    for env_key, val in CHANNELS.items():
        try:
            if val.lstrip('-').isdigit():
                chat = await userbot.get_chat(int(val))
            else:
                chat = await userbot.get_chat(val)
            title = chat.title or chat.username or "N/A"
            print(f"  OK   {env_key:>15} = {val:>25} -> id={chat.id}, title={title}")
            ok.append(env_key)
        except Exception as e:
            print(f"  FAIL {env_key:>15} = {val:>25} -> {e}")
            fail.append((env_key, val, str(e)))

    print(f"\n{'=' * 60}")
    print(f"Result: {len(ok)} OK, {len(fail)} FAIL")
    if fail:
        print("\nFailed channels:")
        for env_key, val, err in fail:
            print(f"  {env_key} = {val}")
            print(f"    Error: {err}")
        print("\nTip: make sure the userbot account is a MEMBER of these channels/groups.")
    else:
        print("\nAll channels resolved successfully!")

    await userbot.stop()


if __name__ == "__main__":
    asyncio.run(main())
