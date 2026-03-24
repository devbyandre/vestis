#!/usr/bin/env python3
# config_utils.py
import json
import os
from typing import Dict, Any

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

DEFAULT = {
    "db_path": os.path.expanduser("portfolio.db"),
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "tax_rate": 0.25,
    "valuation_cache_hours": 24,
    "kpi_cache_hours": 24,
    "yf_max_req_per_min": 45,
    "yf_base_sleep_sec": 0.8,
    "news_max_items": 50,
    "news_min_fetch_minutes": 30,
    "dcf_projection_years": 10,
    "dcf_discount_rate": 0.10,
    "dcf_terminal_growth": 0.025,
    "dcf_conservative": True,
    "dnd": False,

    # --- Holdings suggestions
    "retirement_year": 2099,
    "taxonomy": {
        "basic-materials": [
            "agricultural-inputs", "aluminum", "building-materials", "chemicals",
            "coking-coal", "copper", "gold", "lumber-wood-production",
            "other-industrial-metals-mining", "other-precious-metals-mining",
            "paper-paper-products", "silver", "specialty-chemicals", "steel"
        ],
        "communication-services": [
            "advertising-agencies", "broadcasting", "electronic-gaming-multimedia",
            "entertainment", "internet-content-information", "publishing", "telecom-services"
        ],
        "consumer-cyclical": [
            "apparel-manufacturing", "apparel-retail", "auto-manufacturers", "auto-parts",
            "auto-truck-dealerships", "department-stores", "footwear-accessories",
            "furnishings-fixtures-appliances", "gambling", "home-improvement-retail",
            "internet-retail", "leisure", "lodging", "luxury-goods",
            "packaging-containers", "personal-services", "recreational-vehicles",
            "residential-construction", "resorts-casinos", "restaurants", "specialty-retail",
            "textile-manufacturing", "travel-services"
        ],
        "consumer-defensive": [
            "beverages-brewers", "beverages-non-alcoholic", "beverages-wineries-distilleries",
            "confectioners", "discount-stores", "education-training-services", "farm-products",
            "food-distribution", "grocery-stores", "household-personal-products",
            "packaged-foods", "tobacco"
        ],
        "energy": [
            "oil-gas-drilling", "oil-gas-e-p", "oil-gas-equipment-services",
            "oil-gas-integrated", "oil-gas-midstream", "oil-gas-refining-marketing",
            "thermal-coal", "uranium"
        ],
        "financial-services": [
            "asset-management", "banks-diversified", "banks-regional", "capital-markets",
            "credit-services", "financial-conglomerates", "financial-data-stock-exchanges",
            "insurance-brokers", "insurance-diversified", "insurance-life",
            "insurance-property-casualty", "insurance-reinsurance", "insurance-specialty",
            "mortgage-finance", "shell-companies"
        ],
        "healthcare": [
            "biotechnology", "diagnostics-research", "drug-manufacturers-general",
            "drug-manufacturers-specialty-generic", "health-information-services",
            "healthcare-plans", "medical-care-facilities", "medical-devices",
            "medical-distribution", "medical-instruments-supplies", "pharmaceutical-retailers"
        ],
        "industrials": [
            "aerospace-defense", "airlines", "airports-air-services", "building-products-equipment",
            "business-equipment-supplies", "conglomerates", "consulting-services",
            "electrical-equipment-parts", "engineering-construction",
            "farm-heavy-construction-machinery", "industrial-distribution", "infrastructure-operations",
            "integrated-freight-logistics", "marine-shipping", "metal-fabrication",
            "pollution-treatment-controls", "railroads", "rental-leasing-services",
            "security-protection-services", "specialty-business-services",
            "specialty-industrial-machinery", "staffing-employment-services",
            "tools-accessories", "trucking", "waste-management"
        ],
        "real-estate": [
            "real-estate-development", "real-estate-diversified", "real-estate-services",
            "reit-diversified", "reit-healthcare-facilities", "reit-hotel-motel",
            "reit-industrial", "reit-mortgage", "reit-office", "reit-residential",
            "reit-retail", "reit-specialty"
        ],
        "technology": [
            "communication-equipment", "computer-hardware", "consumer-electronics",
            "electronic-components", "electronics-computer-distribution",
            "information-technology-services", "scientific-technical-instruments",
            "semiconductor-equipment-materials", "semiconductors",
            "software-application", "software-infrastructure", "solar"
        ],
        "utilities": [
            "utilities-diversified", "utilities-independent-power-producers",
            "utilities-regulated-electric", "utilities-regulated-gas",
            "utilities-regulated-water", "utilities-renewable"
        ]
    },
    "target_sector_allocation": {
        "Basic Materials": 0.03,
        "Communication Services": 0.02,
        "Consumer Cyclical": 0.05,
        "Consumer Defensive": 0.25,
        "Energy": 0.05,
        "Financial Services": 0.30,
        "Healthcare": 0.20,
        "Industrials": 0.05,
        "Real Estate": 0.02,
        "Technology": 0.03
    },
    "target_industry_allocation": {},
    "target_risk_profile": {
        "pre_retirement_risk": 0.4,
        "post_retirement_risk": 0.2,
        "market_volatility": 0.15
    },
    "asset_allocation_targets": {
        "pre_retirement": {"Equity": 0.7, "ETF": 0.25, "Bonds": 0.05},
        "post_retirement": {"Equity": 0.5, "ETF": 0.3, "Bonds": 0.2}
    }
}


class _ConfigSingleton:
    _instance = None
    _config: Dict[str, Any]

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self) -> None:
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
                    self._config = json.load(fh)
            except Exception:
                self._config = DEFAULT.copy()
        else:
            self._config = DEFAULT.copy()

        # ensure defaults exist
        for k, v in DEFAULT.items():
            self._config.setdefault(k, v)

    def save(self) -> None:
        with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
            json.dump(self._config, fh, indent=2)

    def get(self, key: str) -> Any:
        return self._config.get(key)

    def set(self, key: str, value: Any) -> None:
        self._config[key] = value
        self.save()

    def all(self) -> Dict[str, Any]:
        return dict(self._config)


# Public API
def get_config(key: str) -> Any:
    # Telegram credentials: prefer environment variables over config.json
    import os as _os
    if key == "telegram_bot_token":
        v = _os.environ.get("TELEGRAM_BOT_TOKEN", "")
        if v: return v
    if key == "telegram_chat_id":
        v = _os.environ.get("TELEGRAM_CHAT_ID", "")
        if v: return v
    return _ConfigSingleton().get(key)


def set_config(key: str, value: Any) -> None:
    # Convert dict/list to JSON string
    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    _ConfigSingleton().set(key, value)


def get_all_config() -> Dict[str, Any]:
    return _ConfigSingleton().all()


def safe_json_load(cfg, default=None):
            if isinstance(cfg, str):
                try:
                    return json.loads(cfg)
                except:
                    return default or {}
            return cfg if isinstance(cfg, dict) else default or {}