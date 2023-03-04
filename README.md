=== Installation ===
```
python3 -m venv 
source venv/bin/activate
pip install -r requirements.txt
```
=== Usage ===
```
python3 download_data_gcp.py -d data "patternag-backend-coding-challenge"
python3 process_data.py -d data -f output.json -b "gs://patternag-backend-coding-challenge"
```
