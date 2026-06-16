def get_provider_info():
    return {
        "package-name": "armada-airflow",
        "name": "Armada Airflow Operator",
        "description": "Armada Airflow Operator.",
        "extra-links": [
            "armada.links.DynamicLink",  # Only needed for Airflow 2.10x
        ],
        "versions": ["1.0.0"],
    }
