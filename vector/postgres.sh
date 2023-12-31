sudo apt-get update
sudo apt-get install --yes postgresql-client
sudo apt install postgresql
sudo service postgresql start
sudo -u postgres psql
#in postgres
CREATE DATABASE vector_embeddings;
CREATE USER your_username WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE vector_embeddings TO mrf;
\q
#log in
psql -U mrf -d vector_embeddings

#set-up vertex extensions
psql "user=mrf dbname=vector_embeddings" -c "CREATE EXTENSION IF NOT EXISTS google_ml_integration CASCADE"
psql "user=mrf dbname=vector_embeddings" -c "CREATE EXTENSION IF NOT EXISTS vector"