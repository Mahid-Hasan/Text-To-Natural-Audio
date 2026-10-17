import asyncio
import os
import subprocess
import shutil
import random
import re
import edge_tts

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================

folder_name = "text_Folder"
chatper_nums = "start_with_sunade"

# 1. Voice Settings
VOICE = "en-US-BrianMultilingualNeural"
RATE = "+30%"       
VOLUME = "+0%"
PITCH = "+0Hz"

# 2. File and Directory Paths
TEXT_FILE_PATH = f"{folder_name}/text/chapter_{chatper_nums}.txt"
FINAL_OUTPUT_FILENAME = f"{folder_name}/audio/chapter_{chatper_nums}.mp3"
TEMP_AUDIO_FOLDER = f"{folder_name}/temp_audio_chunks"
FFMPEG_LIST_FILE = f"{folder_name}/mylist.txt"

# 3. Technical & Performance Settings
API_CHUNK_MAX_LENGTH = 2500  
MAX_RETRIES = 50              
CONCURRENCY_LIMIT = 20        # Reduced to 8 to prevent the script from freezing!

# Fallback text if the file is not found
TEXT_TO_SPEAK = ""

try:
    with open(TEXT_FILE_PATH, "r", encoding="utf-8") as file:
        TEXT_TO_SPEAK = file.read()
    print(f"📖 Loaded text from '{TEXT_FILE_PATH}' ({len(TEXT_TO_SPEAK):,} characters).")
except FileNotFoundError:
    print(f"⚠️ Warning: Could not find '{TEXT_FILE_PATH}'. Using default fallback text.")

# ==============================================================================
# --- HELPER FUNCTIONS ---
# ==============================================================================

def chunk_text(text: str, max_length: int) -> list:
    """
    Cleans and splits text into chunks at natural sentence boundaries.
    """
    text = text.replace('\n', ' ')
    text = ' '.join(text.split())

    chunks = []
    while len(text) > max_length:
        substring = text[:max_length]
        matches = list(re.finditer(r'[.?!]+(?:["\']|\s)', substring))
        
        if matches:
            break_point = matches[-1].end()
        else:
            break_point = substring.rfind(' ')
            if break_point == -1:
                break_point = max_length
                
        chunks.append(text[:break_point].strip())
        text = text[break_point:].strip()
        
    if text:
        chunks.append(text.strip())
        
    print(f"✅ Text divided into {len(chunks)} safe audio chunks.")
    return chunks


async def fetch_audio_for_chunk(semaphore: asyncio.Semaphore, text_chunk: str, part_number: int, save_folder: str) -> str:
    """
    Sends a single chunk to the edge-tts engine.
    Skips the chunk if the audio file already exists (Resume capability).
    """
    async with semaphore:
        output_filename = os.path.join(save_folder, f"part_{part_number:04d}.mp3")
        
        # --- NEW RESUME LOGIC ---
        # If the file already exists and has data (size > 0), skip generating it again!
        if os.path.exists(output_filename) and os.path.getsize(output_filename) > 0:
            print(f"  ⏭️ Chunk {part_number + 1} already exists. Skipping...")
            return output_filename
        # ------------------------
        
        for attempt in range(1, MAX_RETRIES + 1):
            print(f"  > Processing chunk {part_number + 1} (Attempt {attempt}/{MAX_RETRIES})...")
            try:
                communicate = edge_tts.Communicate(
                    text=text_chunk,
                    voice=VOICE,
                    rate=RATE,
                    volume=VOLUME,
                    pitch=PITCH
                )
                
                await communicate.save(output_filename)
                print(f"  ✅ Chunk {part_number + 1} generated successfully!")
                await asyncio.sleep(random.uniform(0.3, 0.7))
                return output_filename

            except Exception as e:
                print(f"  ❌ Error on chunk {part_number + 1} (Attempt {attempt}): {e}")
                if attempt < MAX_RETRIES:
                    wait_time = attempt * 2
                    print(f"  ⏳ Waiting {wait_time}s before retrying...")
                    await asyncio.sleep(wait_time)

        print(f"  🛑 Failed to generate audio for chunk {part_number + 1}.")
        return None


async def download_all_audio_chunks(text_chunks: list, save_folder: str) -> list:
    """
    Manages concurrent downloads using an asyncio Semaphore.
    """
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tasks = [
        fetch_audio_for_chunk(semaphore, chunk, i, save_folder)
        for i, chunk in enumerate(text_chunks)
    ]
    results = await asyncio.gather(*tasks)
    return results

# ==============================================================================
# --- MAIN PIPELINE ---
# ==============================================================================

def main():
    print("--- Starting edge-tts Generation Pipeline ---")
    
    if not TEXT_TO_SPEAK.strip():
        print("❌ Error: No text found to process.")
        return

    final_output_dir = os.path.dirname(FINAL_OUTPUT_FILENAME)
    if final_output_dir and not os.path.exists(final_output_dir):
        os.makedirs(final_output_dir)

    if not os.path.exists(TEMP_AUDIO_FOLDER):
        os.makedirs(TEMP_AUDIO_FOLDER)

    success = False
    try:
        text_chunks = chunk_text(TEXT_TO_SPEAK, API_CHUNK_MAX_LENGTH)
        
        print(f"🚀 Launching requests (Max {CONCURRENCY_LIMIT} concurrent streams)...")
        audio_files_raw = asyncio.run(download_all_audio_chunks(text_chunks, TEMP_AUDIO_FOLDER))
        audio_files = [f for f in audio_files_raw if f is not None]

        if len(audio_files) != len(text_chunks):
            print(f"\n❌ Error: Only {len(audio_files)} out of {len(text_chunks)} chunks were generated. Aborting merge.")
            return

        print(f"\n📝 Writing FFmpeg concatenation file: '{FFMPEG_LIST_FILE}'")
        with open(FFMPEG_LIST_FILE, 'w', encoding='utf-8') as f:
            for audio_file in sorted(audio_files):
                abs_path = os.path.abspath(audio_file)
                formatted_path = abs_path.replace('\\', '/')
                f.write(f"file '{formatted_path}'\n")

        print(f"🧩 Merging {len(audio_files)} audio chunks into '{FINAL_OUTPUT_FILENAME}'...")
        ffmpeg_command = [
            'ffmpeg', '-y', '-f', 'concat', '-safe', '0',
            '-i', FFMPEG_LIST_FILE, '-c', 'copy', FINAL_OUTPUT_FILENAME
        ]

        subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True)
        print(f"🎉 Successfully created final audio: '{FINAL_OUTPUT_FILENAME}'")
        success = True

    except subprocess.CalledProcessError as e:
        print("\n❌ FFmpeg execution failed!")
        print(e.stderr)
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
    finally:
        # --- NEW CLEANUP LOGIC ---
        # Only clean up if the script finished the whole process successfully
        if success:
            print("\n🗑️ Cleaning up temporary chunk files...")
            if os.path.exists(TEMP_AUDIO_FOLDER):
                shutil.rmtree(TEMP_AUDIO_FOLDER)
            if os.path.exists(FFMPEG_LIST_FILE):
                os.remove(FFMPEG_LIST_FILE)
        else:
            print("\n⚠️ Process did not finish successfully.")
            print(f"📁 Temporary files in '{TEMP_AUDIO_FOLDER}' were KEPT so you can resume later.")
        # ------------------------

    if success:
        print(f"✨ Process Complete! Output saved to: {FINAL_OUTPUT_FILENAME}")
        try:
            subprocess.run(["code", FINAL_OUTPUT_FILENAME], check=True)
        except Exception:
            pass

if __name__ == "__main__":
    main()