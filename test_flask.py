"""Test Flask rendering."""
import os
import sys

# Add src/web to path
web_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src', 'web')
sys.path.insert(0, web_dir)

from dashboard import create_app

base_dir = r'c:\Users\wangcheng\workspace\HealthMonitor\tests\sample_metrics'
app = create_app(base_dir)

print(f"Template folder: {app.template_folder}")
print(f"Template exists: {os.path.exists(os.path.join(app.template_folder, 'dashboard.html'))}")
print(f"Template size: {os.path.getsize(os.path.join(app.template_folder, 'dashboard.html'))}")

with app.test_client() as client:
    resp = client.get('/')
    print(f"Response status: {resp.status_code}")
    print(f"Response length: {len(resp.data)}")
    if len(resp.data) > 0:
        print(f"First 200 chars: {resp.data[:200]}")
    else:
        print("Response is EMPTY!")
