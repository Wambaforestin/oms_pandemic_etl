from datetime import datetime


def validate_dataframe(df, required_columns):
    """Valide un DataFrame."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Colonnes manquantes: {missing_columns}")
    return True

def validate_date_format(date_str, format="%Y-%m-%d"):
    """Valide le format d'une date."""
    try:
        datetime.strptime(date_str, format)
        return True
    except ValueError:
        return False