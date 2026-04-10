#!/usr/bin/env bash

# Install Playwright browsers + system deps
playwright install --with-deps chromium

# Run app
streamlit run app.py --server.port=$PORT --server.address=0.0.0.0