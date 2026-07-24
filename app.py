import logging
import os
from quart import Quart
from config import Config
from routes import index, map_data

# Set up logging
log_file_path = '/home/ubuntu/StationaryNetworkServer/logs/quart_app.log'
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)  # Ensure the log directory exists

logging.basicConfig(
    filename=log_file_path,  # Absolute path to the log file
    level=logging.DEBUG,     # Set the logging level
 #   format='%(asctime)s %(levelname)s %(name)s %(message)s'  # Define the log format
    format="%(message)s"  
)
logger = logging.getLogger(__name__)

# Test logging
logger.info("\n\n Logging setup complete. Application is starting.\n\n")

# Initialize the Quart app
app = Quart(__name__)

# Load configuration from the Config class
app.config.from_object(Config)

# Register blueprints
app.register_blueprint(index.bp)
app.register_blueprint(map_data.bp)

# Run the application (for development/testing only)

# The following lines are not required if run with hypercorn but useful for running locally
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
