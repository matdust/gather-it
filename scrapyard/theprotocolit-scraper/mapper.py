SOURCE_SITE_THE_PROTOCOL_IT = 1


def map_offers(offers: list[dict]) -> list[dict]:
    return [map_offer(offer) for offer in offers]


def map_offer(offer: dict) -> dict:
    return {
        "Id": offer["id"],
        "Slug": offer["offerUrlName"],
        "Title": offer["title"],
        "CompanyName": offer["employer"],
        "SourceSite": SOURCE_SITE_THE_PROTOCOL_IT,
        "Skills": offer.get("technologies"),
        "WorkplaceTypes": offer.get("workModes"),
        "ExperienceLevels": _experience_levels(offer),
        "Locations": _locations(offer),
        "Salaries": _salaries(offer),
        "PublishedAt": _published_at(offer),
        "LogoUrl": offer.get("logoUrl"),
    }


def _published_at(offer: dict) -> str | None:
    published_at = offer.get("publicationDateUtc")
    if not published_at:
        return None
    # The API returns UTC timestamps without an offset, which binder rejects
    if published_at.endswith("Z") or "+" in published_at[10:]:
        return published_at
    return f"{published_at}Z"


def _experience_levels(offer: dict) -> list[str] | None:
    position_levels = offer.get("positionLevels")
    if position_levels is None:
        return None
    return [level["value"] for level in position_levels]


def _locations(offer: dict) -> list[str] | None:
    workplaces = offer.get("workplace")
    if workplaces is None:
        return None
    return [
        workplace["city"]
        for workplace in workplaces
        if workplace.get("city") and workplace["city"].strip()
    ]


def _salaries(offer: dict) -> list[dict] | None:
    salaries = [
        {
            "From": contract["salary"].get("from"),
            "To": contract["salary"].get("to"),
            "Currency": contract["salary"].get("currencySymbol") or "",
            "ContractType": contract["salary"].get("kindName"),
        }
        for contract in offer.get("typesOfContracts") or []
        if contract.get("salary")
    ]

    if not salaries and offer.get("salary"):
        salaries = [
            {
                "From": None,
                "To": offer["salary"].get("to"),
                "Currency": offer["salary"].get("currency") or "",
                "ContractType": None,
            }
        ]

    return salaries or None
