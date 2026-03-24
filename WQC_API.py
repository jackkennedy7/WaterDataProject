class WaterQualityClient:
    """Client for https://www.waterqualitydata.us web services"""

    BASE_URL = "https://www.waterqualitydata.us"

    DATASETS = {
        "result":       "/data/Result/search",
        "station":      "/data/Station/search",
        "activity":     "/data/Activity/search",
        "project":      "/data/Project/search",
        "organization": "/data/Organization/search",
        "characteristic": "/data/Characteristic/search",
        "biological":   "/data/BiologicalMetric/search",
        "profile":      "/data/Profile/search",
        "index":        "/data/Index/search",
        "portal":       "/portal",
    }

    def __init__(self, timeout: int = 90):
        self.session = requests.Session()
        self.timeout = timeout

    def list_datasets(self):
        return list(self.DATASETS.keys())

    def build_url(self, dataset: str) -> str:
        dataset = dataset.lower()
        if dataset not in self.DATASETS:
            raise ValueError(f"Unknown dataset '{dataset}'. Available: {self.list_datasets()}")
        return self.BASE_URL + self.DATASETS[dataset]

    def query(self, dataset, params, fmt="csv", stream=False):
        url = self.build_url(dataset)
        params = params.copy()
        params["mimeType"] = "csv" if fmt.lower() == "csv" else "json"
        response = self.session.get(url, params=params, timeout=self.timeout, stream=stream)
        response.raise_for_status()
        return response

    def query_to_dataframe(self, dataset, params):
        response = self.query(dataset, params, fmt="csv", stream=False)
        return pd.read_csv(StringIO(response.text))

    def close(self):
        self.session.close()


# Using the WaterQualityControl class to make a dataframe:
params = {
    "countycode":        "US:37:189",       # ← THREE parts: US:stateCode:countyFIPS
    "statecode":         "US:37",           # North Carolina
    "startDateLo":       "01-01-2015",      # widen window for richer data
    "siteType":          "Stream",
    "sampleMedia":       "Water",
    # characteristicName instead of pCode; request several analytes at once
    "characteristicName": [
        "Dissolved oxygen (DO)",
        "Temperature, water",
        "pH",
        "Specific conductance",
        "Turbidity",
        "Nitrate",
        "Phosphorus",
    ],
}

print("Fetching data from WQP …")
df_raw = client.query_to_dataframe("result", params)
