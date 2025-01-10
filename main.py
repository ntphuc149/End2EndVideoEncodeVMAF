import os
import pandas as pd
from tqdm import tqdm
from conf.config import DBAccess
from utils.utils import DataProcessor, FFmpegCommandGenerator, VMAFCalculator

def main():
   db = DBAccess()
   
   # Get all available codecs
   codecs = db.get_available_codec_names()
   if not codecs:
       print("No active codecs found")
       return
   
   # Setup directories
   data_dir = 'data'
   source_video_dir = os.path.join(data_dir, 's_video')
   encoded_video_dir = os.path.join(data_dir, 'e_video')
   os.makedirs(data_dir, exist_ok=True)
   os.makedirs(encoded_video_dir, exist_ok=True)
   
   # Count total number of encodes
   total_encodes = 0
   for codec in codecs:
       codec_name = codec['master_name']
       profiles = db.get_available_profile_names(codec_name)
       total_encodes += len(profiles) * sum(
           len([f for f in os.listdir(os.path.join(source_video_dir, genre))
                if f.endswith(('.mp4', '.mkv', '.avi', '.mov'))])
           for genre in os.listdir(source_video_dir)
           if os.path.isdir(os.path.join(source_video_dir, genre))
       )

   # Create progress bar
   with tqdm(total=total_encodes, desc="Total Progress") as pbar:
       # Process each video genre
       for genre in os.listdir(source_video_dir):
           genre_path = os.path.join(source_video_dir, genre)
           
           if not os.path.isdir(genre_path):
               continue
               
           print(f"\nProcessing genre: {genre}")
           
           # Process each video in genre
           videos = [f for f in os.listdir(genre_path) 
                    if f.endswith(('.mp4', '.mkv', '.avi', '.mov'))]
           
           for video_file in videos:
               input_video = os.path.join(genre_path, video_file)
               print(f"\nProcessing video: {video_file}")
               
               # Process each codec
               for codec in codecs:
                   codec_name = codec['master_name']
                   profiles = db.get_available_profile_names(codec_name)
                   
                   # Process each profile
                   for profile in profiles:
                       profile_name = profile['name']
                       profile_details = db.get_profile_detail(codec_name, profile_name)
                       
                       if profile_details:
                           profile_data = pd.DataFrame(profile_details)
                           command_data = FFmpegCommandGenerator.generate_ffmpeg_commands_df(profile_data)
                           
                           if not command_data.empty:
                               for _, row in command_data.iterrows():
                                   profile_desc = f"{row['codec']} - {row['profile']} - {row['bitrate']}k"
                                   print(f"\nEncoding {profile_desc}")
                                   
                                   output_name = f"{os.path.splitext(video_file)[0]}_encoded_{row['codec'].replace(' ', '_')}_{row['profile']}_{row['bitrate']}k.yuv"
                                   output_video = os.path.join(encoded_video_dir, genre, output_name)
                                   
                                   ffmpeg_command = FFmpegCommandGenerator.build_ffmpeg_command(
                                       input_file=input_video,
                                       encode_params=row['ffmpeg_cmd'],
                                       codec=row['codec'],
                                       profile=row['profile'],
                                       bitrate=str(row['bitrate']),
                                       genre_folder=genre
                                   )
                                   
                                   if FFmpegCommandGenerator.execute_ffmpeg_command(ffmpeg_command):
                                       print(f"Successfully encoded {profile_desc}")
                                       
                                       # Calculate VMAF score
                                       vmaf_score = VMAFCalculator.calculate_vmaf(
                                           source_path=input_video,
                                           encoded_path=output_video
                                       )
                                       
                                       # Create log entry
                                       log_entry = FFmpegCommandGenerator.create_encoding_log(
                                           input_video=input_video,
                                           output_video=output_video,
                                           ffmpeg_command=ffmpeg_command,
                                           genre_folder=genre
                                       )
                                       
                                       if log_entry:
                                           # Add VMAF score to log entry
                                           log_entry['t_vmaf'] = str(vmaf_score) if vmaf_score is not None else '-'
                                           
                                           # Save to dataset.csv
                                           log_path = os.path.join(data_dir, 'dataset.csv')
                                           DataProcessor.save_profiles_to_csv([log_entry], log_path, mode='a')
                                           
                                       print(f"VMAF Score: {vmaf_score if vmaf_score is not None else 'N/A'}")
                                   else:
                                       print(f"Failed to encode {profile_desc}")
                                       
                                   pbar.update(1)
                                   pbar.set_postfix({'Current': f"{genre}/{video_file}/{profile_desc}"})

   print("\nEncoding process completed!")

if __name__ == "__main__":
   main()