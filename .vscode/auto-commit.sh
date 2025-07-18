#!/bin/bash

# Auto-commit script for professional development workflow
# This script adds all changes and commits with a timestamp

# Get current timestamp
timestamp=$(date "+%Y-%m-%d %H:%M:%S")

# Add all changes
git add .

# Check if there are any changes to commit
if git diff --cached --quiet; then
    echo "No changes to commit"
    exit 0
fi

# Create professional commit message
commit_message="feat: auto-commit changes - $timestamp"

# Commit the changes
git commit -m "$commit_message"

# Push to remote GitHub repository
git push origin HEAD

echo "Auto-commit completed: $commit_message" 