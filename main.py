import http.server
import socketserver
import json
from urllib.parse import urlparse, parse_qs
import threading
import time
import logging
import sys

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Define the slave pool IPs
slave_pool = [f'192.168.0.{i}' for i in range(101, 111)]
allocated_slaves = {}

# Mutex for thread-safe operations on the slave pool
pool_mutex = threading.Lock()


# Function to handle allocation of slaves
def allocate_slaves(amount, duration):
    with pool_mutex:
        if len(slave_pool) >= amount:
            # making new list of the allocated ip's and modifying the slave_pool list
            allocated = [slave_pool.pop(0) for _ in range(amount)]
            for slave in allocated:
                allocated_slaves[slave] = time.time() + duration
            return allocated


# Function to handle the return of slaves
def return_slaves():
    with pool_mutex:
        current_time = time.time()
        returned_slaves = []
        # Iterate over the allocated_slaves dictionary
        for ip, end_time in allocated_slaves.items():
            if end_time <= current_time:
                # If the slave has done, add its IP to the to_return list
                returned_slaves.append(ip)
        # Remove finished slaves from allocated_slaves and return them to the slave pool
        for ip in returned_slaves:
            allocated_slaves.pop(ip)
            slave_pool.append(ip)


# Function to compute the come_back time
def return_wait_time(amount):
    with pool_mutex:
        if allocated_slaves:
            current_time = time.time()
            remaining_times = []
            # Iterate over the allocated_slaves dictionary
            for _, expiry in allocated_slaves.items():
                remaining_time = expiry - current_time
                remaining_times.append(max(remaining_time, 0))  # Ensure non-negative time
            # Calculate and return the minimum remaining time for the desired amount of slaves
            return max(sorted(remaining_times)[:amount])


# HTTP server class
class ServerHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        return_slaves()  # Return expired slaves to the pool
        if not self.path.startswith('/get_slaves'):
            self.send_error(404, "Endpoint not found")
            return

        query_components = dict(parse_qs(urlparse(self.path).query))
        amount_list = query_components.get('amount', ['0'])
        duration_list = query_components.get('duration', ['0'])
        amount_str = amount_list[0]
        duration_str = duration_list[0]

        if not amount_str.isdigit() or not duration_str.isdigit():
            self.send_error(400, "Invalid parameters. 'amount' and 'duration' must be integers.")
            return
        amount = int(amount_str)
        duration = int(duration_str)

        if amount <= 0 or amount > 10:
            self.send_error(400, "Invalid parameters. 'amount' must be greater than '0' and less than '11'.")
            return

        if duration <= 0:
            self.send_error(400, "Invalid parameters. 'duration' must be greater than zero.")
            return

        allocated_slaves = allocate_slaves(amount, duration)
        if allocated_slaves:
            response = {"slaves": allocated_slaves}
            logging.info(f"Allocated slaves: {allocated_slaves}")
        else:
            # Estimate wait time for slaves (simple estimation)
            wait_time = return_wait_time(amount)
            response = {"slaves": [], "come_back": int(wait_time)}
            logging.info("Not enough slaves available. Asked to come back later.")

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())


# Run the server
if __name__ == '__main__':
    port = 8080
    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    with socketserver.TCPServer(("", port), ServerHandler) as httpd:
        logging.info(f"Server started at localhost:{port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        httpd.server_close()
        logging.info("Server stopped.")
