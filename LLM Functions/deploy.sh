# Setup the project (change this to your project ID)
gcloud config set project ba882-team8-fall24

echo "======================================================"
echo "Build the Docker image (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/ba882-team8-fall24/streamlit-rag-app .

echo "======================================================"
echo "Push Docker image to Google Container Registry"
echo "======================================================"

docker push gcr.io/ba882-team8-fall24/streamlit-rag-app

echo "======================================================"
echo "Deploy the application to Cloud Run"
echo "======================================================"

gcloud run deploy streamlit-rag-app \
    --image gcr.io/ba882-team8-fall24/streamlit-rag-app \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --service-account teamproject-phase3@ba882-team8-fall24.iam.gserviceaccount.com \
    --memory 1Gi \
    --port 8080
