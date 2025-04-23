import csv
import datetime
import os
import random

print("Generating synthetic web log data...")

# Ensure output directory exists
os.makedirs('../data/logs', exist_ok=True)
output_file = '../data/logs/web_logs.csv'

# Create a file with 10,000 synthetic web log entries
with open(output_file, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['timestamp', 'ip_address', 'user_agent', 'page', 'status_code', 'bytes'])
    
    # Web pages to simulate
    pages = [
        '/home', '/products', '/category/electronics', '/category/clothing',
        '/product/123', '/product/456', '/cart', '/checkout', '/login',
        '/register', '/account', '/search', '/about', '/contact'
    ]
    
    # Status codes with appropriate distribution
    status_codes = [200, 200, 200, 200, 200, 200, 200, 200, 200, 404, 500, 302, 301]
    
    # User agents (browsers)
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
    ]
    
    # Start date (2 weeks ago)
    start_date = datetime.datetime.now() - datetime.timedelta(days=14)
    
    # Generate log entries
    num_entries = 10000
    for i in range(num_entries):
        timestamp = start_date + datetime.timedelta(
            days=random.randint(0, 14),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        ip = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
        user_agent = random.choice(user_agents)
        page = random.choice(pages)
        status_code = random.choice(status_codes)
        bytes_sent = random.randint(500, 5000) if status_code == 200 else random.randint(100, 400)
        
        writer.writerow([
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            ip,
            user_agent,
            page,
            status_code,
            bytes_sent
        ])
        
        # Show progress
        if (i + 1) % 1000 == 0:
            print(f"Generated {i + 1}/{num_entries} log entries...")

print(f"Successfully generated {num_entries} web log entries")
print(f"Output saved to: {output_file}")
