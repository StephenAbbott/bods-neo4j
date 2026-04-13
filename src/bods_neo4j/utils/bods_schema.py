"""BODS v0.4 schema constants and helpers."""

# BODS v0.4 record types
RECORD_TYPE_ENTITY = "entity"
RECORD_TYPE_PERSON = "person"
RECORD_TYPE_RELATIONSHIP = "relationship"

# BODS v0.4 entity types
ENTITY_TYPES = {
    "registeredEntity",
    "legalEntity",
    "arrangement",
    "anonymousEntity",
    "unknownEntity",
    "state",
    "stateBody",
}

# Entity subtypes (conditional on type)
ENTITY_SUBTYPES = {
    "arrangement": {"trust", "nomination", "other"},
    "legalEntity": {"trust", "other"},
    "stateBody": {"governmentDepartment", "stateAgency", "other"},
}

# Person types
PERSON_TYPES = {"knownPerson", "anonymousPerson", "unknownPerson"}

# Interest types (23 values in BODS v0.4)
INTEREST_TYPES = {
    "shareholding",
    "votingRights",
    "appointmentOfBoard",
    "otherInfluenceOrControl",
    "seniorManagingOfficial",
    "settlor",
    "trustee",
    "protector",
    "beneficiaryOfLegalArrangement",
    "rightsToSurplusAssetsOnDissolution",
    "rightsToProfitOrIncome",
    "rightsGrantedByContract",
    "conditionalRightsGrantedByContract",
    "controlViaCompanyRulesOrArticles",
    "controlByLegalFramework",
    "boardMember",
    "boardChair",
    "unknownInterest",
    "unpublishedInterest",
    "enjoymentAndUseOfAssets",
    "rightToProfitOrIncomeFromAssets",
    "nominee",
    "nominator",
}

# Source types
SOURCE_TYPES = {
    "selfDeclaration",
    "officialRegister",
    "thirdParty",
    "primaryResearch",
    "verified",
}

# Address types
ADDRESS_TYPES = {
    "placeOfBirth",
    "residence",
    "registered",
    "service",
    "alternative",
    "business",
}

# Record status values
RECORD_STATUSES = {"new", "updated", "closed"}

# Unspecified reasons
UNSPECIFIED_REASONS = {
    "noBeneficialOwners",
    "subjectUnableToConfirmOrIdentifyBeneficialOwner",
    "interestedPartyHasNotProvidedInformation",
    "subjectExemptFromDisclosure",
    "interestedPartyExemptFromDisclosure",
    "unknown",
    "informationUnknownToPublisher",
}

# Neo4j label mapping from BODS entity types/subtypes
ENTITY_TYPE_TO_NEO4J_LABEL = {
    "registeredEntity": "RegisteredEntity",
    "legalEntity": "LegalEntity",
    "arrangement": "Arrangement",
    "anonymousEntity": "AnonymousEntity",
    "unknownEntity": "UnknownEntity",
    "state": "State",
    "stateBody": "StateBody",
}

ENTITY_SUBTYPE_TO_NEO4J_LABEL = {
    "trust": "Trust",
    "nomination": "Nomination",
    "governmentDepartment": "GovernmentDepartment",
    "stateAgency": "StateAgency",
}


def get_record_type(statement: dict) -> str:
    """Extract record type from a BODS statement."""
    return statement.get("recordType", "")


def get_record_details(statement: dict) -> dict:
    """Extract record details from a BODS statement."""
    return statement.get("recordDetails", {})


def get_entity_type(record_details: dict) -> str:
    """Extract entity type from entity record details."""
    entity_type = record_details.get("entityType", {})
    return entity_type.get("type", "")


def get_entity_subtype(record_details: dict) -> str:
    """Extract entity subtype from entity record details."""
    entity_type = record_details.get("entityType", {})
    return entity_type.get("subtype", "")


def get_person_type(record_details: dict) -> str:
    """Extract person type from person record details."""
    return record_details.get("personType", "")


def get_neo4j_labels_for_entity(record_details: dict) -> list:
    """Determine Neo4j labels for an entity based on its BODS type/subtype."""
    labels = ["Entity"]
    entity_type = get_entity_type(record_details)
    entity_subtype = get_entity_subtype(record_details)

    if entity_type in ENTITY_TYPE_TO_NEO4J_LABEL:
        labels.append(ENTITY_TYPE_TO_NEO4J_LABEL[entity_type])

    if entity_subtype in ENTITY_SUBTYPE_TO_NEO4J_LABEL:
        labels.append(ENTITY_SUBTYPE_TO_NEO4J_LABEL[entity_subtype])

    return labels


def extract_primary_name(record_details: dict, record_type: str) -> str:
    """Extract the primary name from entity or person record details."""
    if record_type == RECORD_TYPE_ENTITY:
        return record_details.get("name", "")
    elif record_type == RECORD_TYPE_PERSON:
        names = record_details.get("names", [])
        if names:
            # Prefer fullName, then construct from parts
            first_name = names[0]
            if first_name.get("fullName"):
                return first_name["fullName"]
            parts = []
            for part in ["givenName", "patronymicName", "familyName"]:
                if first_name.get(part):
                    parts.append(first_name[part])
            return " ".join(parts) if parts else ""
    return ""


def extract_identifiers(record_details: dict) -> list:
    """Extract identifiers from record details."""
    return record_details.get("identifiers", [])


def extract_addresses(record_details: dict) -> list:
    """Extract addresses from record details."""
    return record_details.get("addresses", [])


def extract_jurisdiction(record_details: dict) -> dict:
    """Extract jurisdiction from entity record details."""
    return record_details.get("jurisdiction", {})


def extract_interests(record_details: dict) -> list:
    """Extract interests from relationship record details."""
    return record_details.get("interests", [])
