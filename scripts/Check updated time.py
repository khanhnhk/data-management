import pytz
from datetime import datetime, timedelta
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive, GoogleDriveFileList, GoogleDriveFile

# gauth = GoogleAuth(
# '/Users/khanhnguyen/Documents/Projects/data-management/scripts/client_secrets.json')
gauth = GoogleAuth()
gauth.LocalWebserverAuth()

drive = GoogleDrive(gauth)

# Get Last 3D in ISO Format:
d = datetime.utcnow() + timedelta(days=-2)
d_with_timezone = d.replace(tzinfo=pytz.UTC)
d_iso = d_with_timezone.isoformat()

file_list = drive.ListFile(
    param={'q': f"modifiedTime >= '{d_iso}''"}).GetList()
print(d_iso)
print(file_list)
