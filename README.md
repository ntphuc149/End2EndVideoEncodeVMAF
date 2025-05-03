# End2EndVideoEncodeVMAF

A comprehensive video encoding automation tool with VMAF (Video Multi-Method Assessment Fusion) quality assessment capabilities. This system automates the process of encoding videos using different codecs and profiles while measuring perceptual quality metrics.

![Python](https://img.shields.io/badge/python-3.x-blue.svg)
![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange.svg)
![FFmpeg](https://img.shields.io/badge/FFmpeg-required-green.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

## 🚀 Features

- **Multi-codec Support**: Encode videos using H.264 and H.265 (HEVC) codecs
- **Profile Management**: Supports various encoding profiles for different use cases
- **Resolution Analysis**: Handles multiple resolutions from 240p to 2160p (4K)
- **VMAF Integration**: Automatically calculates VMAF scores for quality assessment
- **Batch Processing**: Process multiple video files across different genres
- **Database Integration**: MySQL-based codec and profile management
- **Extensive Logging**: Detailed logging for monitoring and debugging
- **CSV Export**: Generates comprehensive encoding data in CSV format

## 📋 Prerequisites

### System Requirements
- Python 3.x
- MySQL 8.0+
- FFmpeg with VMAF library support

### Dependencies
```bash
python-dotenv
mysql-connector-python
pandas
tqdm
```

## 🛠️ Installation

1. **Clone the repository**
```bash
git clone https://github.com/ntphuc149/End2EndVideoEncodeVMAF.git
cd End2EndVideoEncodeVMAF
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env with your database credentials
```

5. **Prepare directories**
```bash
# Create necessary directories
mkdir -p data/s_video/genre_1
mkdir -p data/s_video/genre_2
mkdir -p data/e_video
mkdir -p logs
```

## ⚙️ Configuration

### Environment Variables
The system uses environment variables for configuration. Create a `.env` file with the following variables:

```env
# MySQL Configuration
MYSQL_HOST=your_sql_host
MYSQL_PORT=your_sql_port
MYSQL_DB=your_database
MYSQL_USER=your_username
MYSQL_PASS=your_password

# H.264 Bitrates (kbps)
H264_2160P_BITRATES=[19400,19600,19800,20000,20200,20400,20600]
H264_1440P_BITRATES=[9400,9600,9800,10000,10200,10400,10600]
H264_1080P_BITRATES=[3800,4000,4200,4400,4600,4800,5000]
# ... (add remaining bitrate configurations)
```

### Directory Structure
```
End2EndVideoEncodeVMAF/
├── conf/
│   ├── config.py           # Database and configuration management
│   └── log_config.py       # Logging configuration
├── data/
│   ├── s_video/           # Source videos (organized by genre)
│   ├── e_video/           # Encoded output videos
│   └── dataset.csv        # Encoding results log
├── logs/
│   └── log.log            # Application logs
├── process/
│   └── calculate_vmaf.py  # VMAF calculation utilities
├── utils/
│   └── utils.py           # Core utility functions
├── main.py                # Main application entry point
├── requirements.txt       # Project dependencies
└── README.md             # Project documentation
```

## 🚀 Usage

### Basic Usage
```bash
python main.py
```

### How It Works

1. **Initialization**: The system connects to MySQL database to retrieve available codecs and profiles
2. **Video Processing**: Iterates through source videos in different genre folders
3. **Encoding**: For each video, applies different codec/profile combinations with various bitrates
4. **Quality Assessment**: Calculates VMAF scores comparing original and encoded videos
5. **Data Collection**: Saves encoding results and VMAF scores to CSV for analysis

### Input Video Placement
Place your source videos in the appropriate genre folders:
```
data/s_video/action/video1.mp4
data/s_video/drama/video2.mp4
data/s_video/comedy/video3.mp4
```

### Output
- Encoded videos: `data/e_video/{genre}/{video_name}_encoded_{codec}_{profile}_{bitrate}k.yuv`
- Encoding log: `data/dataset.csv`
- Application logs: `logs/log.log`

## 📊 Output Data Format

The system generates a CSV file with the following columns:

| Column | Description |
|--------|-------------|
| s_name | Source video filename |
| s_width | Source video width |
| s_height | Source video height |
| s_size | Source video file size |
| s_duration | Source video duration |
| s_scan_type | Scan type (progressive/interlaced) |
| s_content_type | Video genre/category |
| e_width | Encoded video width |
| e_height | Encoded video height |
| e_aspect_ratio | Encoded video aspect ratio |
| e_pixel_aspect_ratio | Pixel aspect ratio |
| e_codec | Codec used (h264/h265) |
| e_codec_profile | Encoding profile used |
| e_codec_level | Codec level |
| e_framerate | Frame rate |
| e_gop_size | GOP size |
| e_b_frame_int | B-frame interval |
| e_scan_type | Scan type |
| e_bit_depth | Bit depth |
| e_pixel_fmt | Pixel format |
| e_bitrate | Target bitrate |
| e_max_bitrate | Maximum bitrate |
| e_buffer_size | Buffer size |
| e_size | Encoded file size |
| e_duration | Encoded video duration |
| t_vmaf | VMAF score |

## 🔧 Database Schema

The system requires MySQL stored procedures:
- `get_available_codec_name()`: Retrieves available codecs
- `get_available_profile_name(codec_name)`: Gets profiles for a specific codec
- `get_profile_detail(codec_name, profile_name)`: Gets detailed profile parameters

## 🛠️ Development

### Class Architecture

1. **Config**: Database configuration and validation
2. **MySqlConnectionPool**: Singleton connection pool manager
3. **DBAccess**: Database access layer
4. **DataProcessor**: CSV data handling
5. **FFmpegCommandGenerator**: FFmpeg command generation and execution
6. **VideoAnalyzer**: Video analysis utilities
7. **VMAFCalculator**: VMAF score calculation

### Extending the System

To add new codecs or modify encoding parameters:
1. Update database profiles
2. Add appropriate bitrate ranges in `.env`
3. Extend `FFmpegCommandGenerator` if needed

## 📝 Logging

The system provides comprehensive logging:
- **Debug level**: Detailed operation information
- **Info level**: General status updates
- **Error level**: Error conditions and exceptions
- **Log rotation**: Daily log file rotation

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

**Truong-Phuc Nguyen**
- GitHub: [@ntphuc149](https://github.com/ntphuc149)

## 🙏 Acknowledgments

- FFmpeg community for the powerful encoding tools
- Netflix for the VMAF metric
- Contributors and testers

## 📞 Support

For support, please open an issue in the GitHub repository.

---

**Note**: This tool is designed for research and quality analysis purposes. Ensure you have proper licensing for any content you process with this system.
