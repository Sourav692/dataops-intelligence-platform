FROM databricks/databricks-runtime:13.3-LTS-py

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY src/ src/
COPY config/ config/
COPY sql/ sql/
COPY notebooks/ notebooks/

# Set Python path
ENV PYTHONPATH="/app:$PYTHONPATH"

# Default command
CMD ["python", "-m", "src.dip.orchestrator"]
