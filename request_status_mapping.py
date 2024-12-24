import json
from bidi.algorithm import get_display
import unicodedata

def normalize_hebrew(text):
    """Normalize and format Hebrew text for proper RTL display."""
    return get_display(unicodedata.normalize("NFKC", text.strip()))

# Define the mapping dictionary
request_status_mapping = {
    1: normalize_hebrew("ממתין לתשלום"),
    2: normalize_hebrew("ממתין לקליטת תשלום"),
    3: normalize_hebrew("ממתין לקליטת מסמכים"),
    4: normalize_hebrew("ממתין לקליטת מסמכים"),
    5: normalize_hebrew("בדיקת מזכירות"),
    6: normalize_hebrew("ניתוב לדיין"),
    7: normalize_hebrew("ממתין לקביעת דיון"),
    8: normalize_hebrew("ממתין לדיון"),
    9: normalize_hebrew("ממתין להחלטה"),
    10: normalize_hebrew("ממתין להשלמת מסמכים"),
    11: normalize_hebrew("ממתין לתגובת צד א'"),
    12: normalize_hebrew("ממתין לתגובת צד ב'"),
    13: normalize_hebrew("ממתין לתגובת הצדדים"),
    14: normalize_hebrew("תיק מתנהל"),
    15: normalize_hebrew("ניתן פסק דין"),
    16: normalize_hebrew("סגור"),
    17: normalize_hebrew("מבוטל"),
    18: normalize_hebrew("קליטה הסתיימה"),
    19: normalize_hebrew("תקלה בקליטת מסמכים"),
    20: normalize_hebrew("בדיקת מסמכים"),
    21: normalize_hebrew("ממתין לקביעת דיון"),
    22: normalize_hebrew("ממתין לקליטת מסמכים"),
    24: normalize_hebrew("אוחד"),
    25: normalize_hebrew("ממתין לתגובת הצדדים - עורר"),
    26: normalize_hebrew("ממתין לתגובת הצדדים - משיבה"),
    27: normalize_hebrew("תיק מוקפא / מעוכב"),
    28: normalize_hebrew("המשך הליכים בתיק")
}
