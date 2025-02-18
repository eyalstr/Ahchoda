import json
from bidi.algorithm import get_display
import unicodedata

def normalize_hebrew(text):
    """Normalize and format Hebrew text for proper RTL display."""
    return get_display(unicodedata.normalize("NFKC", text.strip()))

decision_type_mapping = {
    1: normalize_hebrew("תביעה / בקשה התקבלה - החלטה סופית"),
    2: normalize_hebrew("תביעה / בקשה נדחתה - החלטה סופית"),
    3: normalize_hebrew("תביעה / בקשה נמחקה"),
    4: normalize_hebrew("ניתן פסק דין גירושין עם עדה"),
    5: normalize_hebrew("ניתן פסק דין הוכחת / אישור נישואין"),
    6: normalize_hebrew("הצדדים התפשרו"),
    7: normalize_hebrew("ניתן פסק דין הוכחת / אישור נישואין (ביגמיה)"),
    8: normalize_hebrew("ניתן פסק דין הוכחת / אישור נישואין (קטינים)"),
    9: normalize_hebrew("ניתן פסק דין גירושין בלי עדה"),
    10: normalize_hebrew("ניתן פסק דין גירושין"),
    11: normalize_hebrew("ערעור התקבל - תיק הוחזר לבית הדין"),
    12: normalize_hebrew("ערעור התקבל - החלטה סופית"),
    13: normalize_hebrew("ערעור נדחה"),
    14: normalize_hebrew("ערעור נדחה - הוחזר לבית הדין"),
    15: normalize_hebrew("הועבר לבית דין אחר"),
    16: normalize_hebrew("ערעור לא נידון (נמחק)"),
    17: normalize_hebrew("הצדדים התפשרו"),
    18: normalize_hebrew("ניתן חוות דעת"),
    19: normalize_hebrew("הבקשה נדחתה"),
    20: normalize_hebrew("הבקשה התקבלה"),
    21: normalize_hebrew("ערעור נמחק ללא דיון"),
    22: normalize_hebrew("ערעור נמחק"),
    23: normalize_hebrew("החלטה"),
    24: normalize_hebrew("פסק דין"),
    25: normalize_hebrew("צו"),
    26: normalize_hebrew("הודעת מזכירות"),
    27: normalize_hebrew("צו לסיכומים"),
    28: normalize_hebrew("הודעה"),
    29: normalize_hebrew("תגובת העורר"),
    30: normalize_hebrew("תגובת המשיבה"),
    31: normalize_hebrew("תגובת הצדדים"),
    32: normalize_hebrew("דחיה על הסף"),
    33: normalize_hebrew("ערר התקבל"),
    34: normalize_hebrew("ערר נדחה"),
    35: normalize_hebrew("פשרה - הודעה מטעם הצדדים"),
    36: normalize_hebrew("פשרה - לאחר דיון"),
    37: normalize_hebrew("סגירה מנהלית"),
    38: normalize_hebrew("מחיקה לבקשת עורר"),
    39: normalize_hebrew("קביעת דיון"),
    40: normalize_hebrew("שינוי מועד דיון"),
    41: normalize_hebrew("ביטול וקביעת דיון בהמשך"),
    42: normalize_hebrew("ביטול דיון"),
    43: normalize_hebrew("איסור פרסום חלקי"),
    44: normalize_hebrew("איסור פרסום"),
    45: normalize_hebrew("דלתיים סגורות"),
    46: normalize_hebrew("הוצאות"),
    47: normalize_hebrew("החלטה למתן פסד"),
    48: normalize_hebrew("תיקון טעות בהחלטה/פרוטוקול"),
    49: normalize_hebrew("המצאת כתבי בית הדין באופן לא מקוון"),
    50: normalize_hebrew("החלטה כללית"),
    51: normalize_hebrew("החלטה לאישור הגשת ערר באמצעים לא מקוונים"),
    52: normalize_hebrew("דחיה"),
    53: normalize_hebrew("אישור"),
    57: normalize_hebrew("החזרה לרשות"),
    58: normalize_hebrew("כתב תשובה / הגנה"),
    59: normalize_hebrew("איחוד תיקים"),
    60: normalize_hebrew("שינוי מותב"),
    70: normalize_hebrew("החלטה על קביעת / שינוי / ביטול דיון"),
    71: normalize_hebrew("הפצת פרוטוקול דיון"),
    72: normalize_hebrew("החלטה בפשרה"),
    73: normalize_hebrew("הארכת מועד להגשת ערר"),
    74: normalize_hebrew("בקשה להארכת מועד להגשת תשובה"),
    75: normalize_hebrew("המצאת כתבי בית הדין באופן לא מקוון"),
    76: normalize_hebrew("הוספת מסמכים"),
    77: normalize_hebrew("שינוי מקום שיפוט"),
    78: normalize_hebrew("בקשה לעיון במסמכים"),
    79: normalize_hebrew("הוצאות"),
    80: normalize_hebrew("המשך הליכים"),
    81: normalize_hebrew("אושר פטור"),
    82: normalize_hebrew("נדחה פטור"),
    83: normalize_hebrew("ביניים - ממתין להשלמת מסמכים"),
    84: normalize_hebrew("שינוי מותב"),
    85: normalize_hebrew("איחוד תיקים")
}

judge_tasks_mapping = {
    "עיון והחלטה":normalize_hebrew("ממתין לעיון ראשוני"),
    "החלטה לאחר דיון":normalize_hebrew("ממתין להחלטה לאחר דיון"),
    "החלטה על תגובה בחריגה": normalize_hebrew("ממתין להחלטה בחריגה"),
    "החלטה בבקשה חדשה": normalize_hebrew("ממתין להחלטה בבקשה"),
    "החלטה על תגובה": normalize_hebrew("ממתין להחלטה בתגובה")
}


other_tasks_mapping = {
    "מטלה לכתב תשובה":normalize_hebrew("ממתין לכתב תשובה משיב"),
    "מטלה לתגובת צד ב":normalize_hebrew("ממתין לתגובת צד ב"),
    "מטלה לתגובת צד א":normalize_hebrew("ממתין לתגובת צד א") ,
    "מטלה לתגובת צד א' מתוך תגובת הצדדים":normalize_hebrew("ממתין לתגובת צד א' מתוך תגובת הצדדים") ,
    "מטלה לתגובת צד ב' מתוך תגובת הצדדים":normalize_hebrew("ממתין לתגובת צד ב' מתוך תגובת הצדדים") ,
    "ממתין להשלמת מסמכים":normalize_hebrew("ממתין להשלמת מסמכים")
    
    
      
}

secratary_tasks_mapping = {
    "ביטול דיון": normalize_hebrew("ממתין לביטול דיון"),
    "קביעת דיון": normalize_hebrew("ממתין לקביעת מועד דיון"),
    "בדיקת מזכירות": normalize_hebrew("ממתין לבדיקת מזכירות"),
    "שינוי מועד דיון": normalize_hebrew("ממתין לשינוי מועד דיון")
}