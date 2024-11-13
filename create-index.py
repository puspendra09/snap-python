from elasticsearch import Elasticsearch

# Elasticsearch Configuration
es_host = "https://164.152.28.147:9200"  # Replace with your Elasticsearch host
es_username = "snapapp_es"                # Replace with your Elasticsearch username
es_password = "rjPXPR5FD153Issw0rd"       # Replace with your Elasticsearch password

# Connect to Elasticsearch with authentication
es = Elasticsearch(
    [es_host],
    basic_auth=(es_username, es_password),
    verify_certs=False  # Set to True in production if using valid SSL certificates
)

# Define the index settings and mappings
index_name = "resume_new"
index_body = {
    "mappings": {
        "properties": {
            "Resume_text": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "Resume_url": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "content": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "coverLetter": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "created": {
                "type": "date"
            },
            "email": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "id": {
                "type": "long"
            },
            "name": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "resumeText": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "s3": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "text": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            }
        }
    }
}

# Create the index
try:
    response = es.indices.create(index=index_name, body=index_body)
    print(f"Index '{index_name}' created successfully: {response}")
except Exception as e:
    print(f"Error creating index: {e}")