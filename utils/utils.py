import os
import ast
import time
import json
import subprocess
import pandas as pd
from typing import List, Dict
from conf.log_config import logger

class DataProcessor:
    @staticmethod
    def save_profiles_to_csv(data: List[Dict], output_path: str, mode: str = 'a') -> bool:
        """
        Save data to CSV file
        Args:
            data: List of dictionaries containing data
            output_path: Path to output CSV file
            mode: 'w' for write (create new), 'a' for append
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not data:
                logger.warning("No data to save")
                return False
            
            df = pd.DataFrame(data)
            
            # Check if file exists and mode is append
            if mode == 'a' and os.path.exists(output_path):
                df.to_csv(output_path, mode='a', header=False, index=False, encoding='utf-8')
            else:
                # If file doesn't exist or mode is write, create new file
                df.to_csv(output_path, index=False, encoding='utf-8')
            
            logger.info(f"Successfully saved data to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False
            
class FFmpegCommandGenerator:
    @staticmethod
    def _get_resolution_key(resolution: str) -> str:
        if 'x2160' in resolution:
            return '2160p'
        elif 'x1440' in resolution:
            return '1440p'
        elif 'x1080' in resolution:
            return '1080p'
        elif 'x720' in resolution:
            return '720p'
        elif 'x480' in resolution:
            return '480p'
        elif 'x360' in resolution:
            return '360p'
        elif 'x240' in resolution:
            return '240p'
        return None

    @staticmethod
    def _get_bitrate_ranges():
        """Get bitrate ranges from environment variables"""
        try:
            bitrate_ranges = {
                'h264 master': {
                    '2160p': ast.literal_eval(os.getenv('H264_2160P_BITRATES')),
                    '1440p': ast.literal_eval(os.getenv('H264_1440P_BITRATES')),
                    '1080p': ast.literal_eval(os.getenv('H264_1080P_BITRATES')),
                    '720p': ast.literal_eval(os.getenv('H264_720P_BITRATES')),
                    '480p': ast.literal_eval(os.getenv('H264_480P_BITRATES')),
                    '360p': ast.literal_eval(os.getenv('H264_360P_BITRATES')),
                    '240p': ast.literal_eval(os.getenv('H264_240P_BITRATES'))
                },
                'h265 master': {
                    '2160p': ast.literal_eval(os.getenv('H265_2160P_BITRATES')),
                    '1440p': ast.literal_eval(os.getenv('H265_1440P_BITRATES')),
                    '1080p': ast.literal_eval(os.getenv('H265_1080P_BITRATES')),
                    '720p': ast.literal_eval(os.getenv('H265_720P_BITRATES')),
                    '480p': ast.literal_eval(os.getenv('H265_480P_BITRATES')),
                    '360p': ast.literal_eval(os.getenv('H265_360P_BITRATES'))
                }
            }
            return bitrate_ranges
        except Exception as e:
            logger.error(f"Error loading bitrate ranges from environment: {e}")
            return {}

    @staticmethod
    def generate_ffmpeg_commands_df(df: pd.DataFrame) -> pd.DataFrame:
        # Get bitrate ranges from environment
        bitrate_ranges = FFmpegCommandGenerator._get_bitrate_ranges()
        
        commands_list = []
        try:
            for codec_name in df['master_name'].unique():
                for profile_name in df[df['master_name'] == codec_name]['name'].unique():
                    profile_data = df[(df['master_name'] == codec_name) & 
                                    (df['name'] == profile_name)].copy()
                    
                    # Get resolution
                    resolution = profile_data[profile_data['pro_key'] == '-s']['pro_value'].iloc[0] if not profile_data[profile_data['pro_key'] == '-s'].empty else 'N/A'
                    res_key = FFmpegCommandGenerator._get_resolution_key(resolution)

                    if codec_name in bitrate_ranges and res_key in bitrate_ranges[codec_name]:
                        filtered_data = profile_data[~profile_data['pro_key'].isin([
                            '-extention', '-f'
                        ])]
                        
                        base_params = dict(zip(filtered_data['pro_key'], filtered_data['pro_value']))
                        
                        # Loop bitrate in list
                        for bitrate in bitrate_ranges[codec_name][res_key]:
                            command = f""
                            for key, value in base_params.items():
                                if key.startswith('-'):
                                    command += f" {key} {value}"
                                else:
                                    command += f" -{key} {value}"
                            
                            # Calculating additional bitrate
                            maxrate = int(bitrate * 1.5)
                            bufsize = bitrate * 2
                            command += f" -b:v {bitrate}k -maxrate {maxrate}k -bufsize {bufsize}k"
                            
                            commands_list.append({
                                'codec': codec_name,
                                'profile': profile_name,
                                'resolution': resolution,
                                'bitrate': bitrate,
                                'ffmpeg_cmd': command
                            })
                    else:
                        filtered_data = profile_data[~profile_data['pro_key'].isin([
                            '-extention', '-f'
                        ])]
                        
                        params = dict(zip(filtered_data['pro_key'], filtered_data['pro_value']))
                        command = f""
                        for key, value in params.items():
                            if key.startswith('-'):
                                command += f" {key} {value}"
                            else:
                                command += f" -{key} {value}"
                        
                        commands_list.append({
                            'codec': codec_name,
                            'profile': profile_name,
                            'resolution': resolution,
                            'bitrate': '-',
                            'ffmpeg_cmd': command
                        })
                        
                    logger.info(f"Successfully generated FFmpeg commands for codec: {codec_name}, profile: {profile_name}")
            
            return pd.DataFrame(commands_list)
        except Exception as e:
            logger.error(f"Error generating FFmpeg commands DataFrame: {e}")
            return pd.DataFrame()

    @staticmethod
    def build_ffmpeg_command(input_file: str, encode_params: str, codec: str, profile: str, 
                           bitrate: str, genre_folder: str = None) -> str:
        input_dir = os.path.dirname(input_file)
        input_name = os.path.basename(input_file)
        base_output_dir = os.path.join(os.path.dirname(os.path.dirname(input_dir)), 'e_video')
        
        if genre_folder:
            output_dir = os.path.join(base_output_dir, genre_folder)
            os.makedirs(output_dir, exist_ok=True)
        else:
            output_dir = base_output_dir
        
        name = os.path.splitext(input_name)[0]
        clean_codec = codec.replace(' ', '_').replace('-', '_')
        clean_profile = profile.replace(' ', '_').replace('-', '_')
        
        # Add bitrate suffix in file name for easy recognize
        bitrate_str = f"_{bitrate}k" if bitrate != '-' else ''
        output_name = f"{name}_encoded_{clean_codec}_{clean_profile}{bitrate_str}.yuv"
        output_path = os.path.join(output_dir, output_name)
        
        # return f"ffmpeg -i {input_file} -pix_fmt yuv420p {encode_params} -f yuv4mpegpipe {output_path}"
        return f"ffmpeg -i {input_file} {encode_params} {output_path}"


    @staticmethod
    def execute_ffmpeg_command(command: str) -> bool:
        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                logger.info(f"Successfully executed FFmpeg command")
                return True
            else:
                logger.error(f"FFmpeg command failed with error: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error executing FFmpeg command: {e}")
            return False
        
    @staticmethod
    def create_encoding_log(input_video: str, 
                        output_video: str,
                        ffmpeg_command: str,
                        genre_folder: str) -> Dict:
        try:
            default_log = {
                # Source video info
                's_name': '-',
                's_width': '-',
                's_height': '-',
                's_size': '-',
                's_duration': '-',
                's_scan_type': '-',
                's_content_type': '-',
                
                # Encoded video info
                'e_width': '-',
                'e_height': '-',
                'e_aspect_ratio': '-',
                'e_pixel_aspect_ratio': '-',
                'e_codec': '-',
                'e_codec_profile': '-',
                'e_codec_level': '-',
                'e_framerate': '-',
                'e_gop_size': '-',
                'e_b_frame_int': '-',
                'e_scan_type': '-',
                'e_bit_depth': '-',
                'e_pixel_fmt': '-',
                'e_bitrate': '-',
                'e_max_bitrate': '-',
                'e_buffer_size': '-',
                'e_size': '-',
                'e_duration': '-',
                't_vmaf': '-'
            }
            
            # Collecting info from variety source
            source_info = VideoAnalyzer.get_source_video_info(input_video, genre_folder)
            encode_params = VideoAnalyzer.parse_ffmpeg_command(ffmpeg_command)
            encoded_info = VideoAnalyzer.get_encoded_video_info(output_video)
            
            # Update default_log with information
            default_log.update(source_info)
            default_log.update(encode_params)
            default_log.update(encoded_info)
            
            return default_log
        except Exception as e:
            logger.error(f"Error creating data file: {e}")
            return None

    @staticmethod
    def save_encoding_log(log_entries: List[Dict], output_path: str) -> bool:
        """Save data into CSV"""
        try:
            df = pd.DataFrame(log_entries)
            df.to_csv(output_path, index=False)
            return True
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            return False
        
class VideoAnalyzer:
    @staticmethod
    def get_source_video_info(video_path: str, content_type: str) -> Dict:
        """Get video info using ffprobe"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                video_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if not result.stdout:
                logger.warning(f"No output from ffprobe for {video_path}")
                return {}
                
            data = json.loads(result.stdout)
            
            video_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
            if not video_stream:
                logger.warning(f"No video stream found in {video_path}")
                return {}
                
            format_info = data.get('format', {})
            
            return {
                's_name': os.path.basename(video_path),
                's_width': str(video_stream.get('width', '-')),
                's_height': str(video_stream.get('height', '-')),
                's_size': str(format_info.get('size', '-')),
                's_duration': str(format_info.get('duration', '-')),
                's_scan_type': video_stream.get('field_order', 'progressive'),
                's_content_type': content_type
            }
        except Exception as e:
            logger.error(f"Error analyzing source video: {e}")
            return {}
        
    @staticmethod
    def parse_ffmpeg_command(command: str) -> Dict:
        """Extract FFmpeg command to get encoding detail"""
        params = {}
        try:
            # Spliting command elements
            parts = command.split()
            
            # Parse parameters
            for i, part in enumerate(parts):
                if part == '-s':
                    width, height = parts[i+1].split('x')
                    params['e_width'] = int(width)
                    params['e_height'] = int(height)
                elif part == '-aspect':
                    params['e_aspect_ratio'] = parts[i+1]
                elif part == '-pix_fmt':
                    params['e_pixel_fmt'] = parts[i+1]
                elif part == '-r':
                    params['e_framerate'] = int(parts[i+1])
                elif part == '-b:v':
                    params['e_bitrate'] = parts[i+1]
                elif part == '-maxrate':
                    params['e_max_bitrate'] = parts[i+1]
                elif part == '-bufsize':
                    params['e_buffer_size'] = parts[i+1]
                elif part == '-profile:v':
                    params['e_codec_profile'] = parts[i+1]
                elif part == '-level':
                    params['e_codec_level'] = parts[i+1]
                elif part == '-bf':
                    params['e_b_frame_int'] = int(parts[i+1])
                
                # Parse x264/x265 params
                if 'x264opts' in part or 'x265-params' in part:
                    codec_params = parts[i+1].split(':')
                    for param in codec_params:
                        key, value = param.split('=')
                        if key == 'keyint':
                            params['e_gop_size'] = int(value)
                
                # Determine codec
                if '-c:v libx264' in command:
                    params['e_codec'] = 'h264'
                elif '-c:v libx265' in command:
                    params['e_codec'] = 'h265'
                
            # Set default values
            params['e_pixel_aspect_ratio'] = '1:1'
            params['e_scan_type'] = 'progressive'
            params['e_bit_depth'] = 8  # Default for yuv420p
            params['t_vmaf'] = ''
            
            return params
        except Exception as e:
            logger.error(f"Error parsing FFmpeg command: {e}")
            return {}

    @staticmethod
    def get_encoded_video_info(video_path: str, max_retries: int = 3, delay: float = 1.0) -> Dict:
        """Get video encode infomation after retrieve"""
        default_info = {
            'e_size': '-',
            'e_duration': '-'
        }
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    time.sleep(delay)
                    
                cmd = [
                    'ffprobe',
                    '-v', 'quiet',
                    '-print_format', 'json',
                    '-show_format',
                    '-show_streams',
                    video_path
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                if not result.stdout:
                    continue
                    
                data = json.loads(result.stdout)
                video_stream = next((s for s in data.get('streams', []) if s.get('codec_type') == 'video'), None)
                if not video_stream:
                    continue
                    
                format_info = data.get('format', {})
                
                return {
                    'e_size': str(format_info.get('size', '-')),
                    'e_duration': str(format_info.get('duration', '-'))
                }
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    return default_info
                continue
        
        return default_info
    
class VMAFCalculator:
    @staticmethod
    def get_video_resolution(video_path: str) -> tuple:
        """Get video resolution using ffprobe"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_streams',
                video_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            data = json.loads(result.stdout)
            
            video_stream = next((s for s in data['streams'] if s['codec_type'] == 'video'), None)
            if video_stream:
                return (int(video_stream['width']), int(video_stream['height']))
            return None
        except Exception as e:
            logger.error(f"Error getting video resolution: {e}")
            return None

    @staticmethod
    def calculate_vmaf(source_path: str, encoded_path: str) -> float:
        """Calculate VMAF score between source and encoded video"""
        try:
            # Get source and encode video resolution
            source_res = VMAFCalculator.get_video_resolution(source_path)
            encoded_res = VMAFCalculator.get_video_resolution(encoded_path)
            
            if not source_res or not encoded_res:
                return None
            
            # Add filter libvmaf
            if source_res != encoded_res:
                filter_complex = f"[1]scale={source_res[0]}:{source_res[1]}[scaled];[0][scaled]libvmaf=model=version=vmaf_v0.6.1:n_threads=8"
            else:
                filter_complex = "libvmaf=model=version=vmaf_v0.6.1:n_threads=8"
            
            # Run FFMPEG command with libvmaf
            cmd = [
                'ffmpeg',
                '-i', source_path,
                '-i', encoded_path,
                '-filter_complex', filter_complex,
                '-f', 'null',
                '-'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # Get VMAF score
            vmaf_score = None
            for line in result.stderr.split('\n'):
                if 'VMAF score:' in line:
                    vmaf_score = float(line.split(':')[-1].strip())
                    break
            
            return vmaf_score
            
        except Exception as e:
            logger.error(f"Error calculating VMAF: {e}")
            return None