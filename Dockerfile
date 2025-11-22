FROM coady/pylucene:10.0.0

# Set working directory
WORKDIR /app

COPY index_joined_data_lucene.py /app/index_joined_data_lucene.py
COPY search.py /app/search.py

RUN chmod +x /app/index_joined_data_lucene.py /app/search.py

# Default entrypoint
ENTRYPOINT ["python3"]

