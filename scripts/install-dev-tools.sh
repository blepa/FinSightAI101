pip install --upgrade pip 
pip install -r ./scripts/requirements.txt

# Verification 
java --version
python --version
spark-submit --version

# Update apt-get
apt-get update 
# Install git
apt-get install -y git
# Clean up to reduce image size
apt-get clean && rm -rf /var/lib/apt/lists/

# Enable git
git config --global --add safe.directory /workspaces/*
