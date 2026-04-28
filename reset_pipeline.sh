# Buat file reset_pipeline.sh
cat > ~/projects/data-pipeline/reset_pipeline.sh << 'EOF'
#!/bin/bash
echo "Resetting pipeline hash..."
docker compose exec postgres psql -U airflow -d airflow -c \
  "DELETE FROM meta.pipeline_state WHERE source = 'csv';"

echo "Triggering DAG with force=true..."
curl -s -u admin:admin123 \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"conf": {"force": true}}' \
  "http://localhost:8080/api/v1/dags/etl_pipeline/dagRuns"

echo "Done! Check http://localhost:8080"
EOF

chmod +x ~/projects/data-pipeline/reset_pipeline.sh

# Setiap ganti dataset tinggal jalankan:
# cd ~/projects/data-pipeline./reset_pipeline.sh
# 