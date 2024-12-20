from bidi.algorithm import get_display
import unicodedata

def normalize_hebrew(text):
    """Normalize and format Hebrew text for proper RTL display."""
    return get_display(unicodedata.normalize("NFKC", text.strip()))

DOCUMENT_TYPE_MAPPING = {
      1: normalize_hebrew("אגרה"),
    2: normalize_hebrew("אישור תשלום שירות פרסום"),
    3: normalize_hebrew("שובר תשלום פקדון"),
    4: normalize_hebrew("אגרת הליך ביניים"),
    5: normalize_hebrew("הודעה לבית הדין"),
    6: normalize_hebrew("כתב תביעה"),
    7: normalize_hebrew("כתב הגנה"),
    8: normalize_hebrew("כתב תביעה מתוקן"),
    9: normalize_hebrew("תעודה מזהה"),
    10: normalize_hebrew("חוזה נישואין"),
    11: normalize_hebrew("הסכם גירושין"),
    12: normalize_hebrew("החלטת נישואין מאושרת של בי\"ד שרעי"),
    13: normalize_hebrew("תעודת גירושין"),
    14: normalize_hebrew("תעודת פטירה"),
    15: normalize_hebrew("ייפוי כוח"),
    16: normalize_hebrew("תגובה"),
    17: normalize_hebrew("הוכחה"),
    18: normalize_hebrew("אישור רפואי"),
    19: normalize_hebrew("אישור מילואים"),
    20: normalize_hebrew("אישור לידה"),
    21: normalize_hebrew("הצהרת פטירה"),
    22: normalize_hebrew("הזמנה לדיון"),
    23: normalize_hebrew("אחר"),
    24: normalize_hebrew("שיק מבוטל"),
    25: normalize_hebrew("פרטי חשבון בנק"),
    26: normalize_hebrew("תדפיס חשבון בנק"),
    27: normalize_hebrew("רשיון רכב"),
    28: normalize_hebrew("אישור הכנסות מעבודה"),
    29: normalize_hebrew("אישור מהרווחה"),
    30: normalize_hebrew("הסכמת היורשים על סמכות בית הדין לחלוקה שרעית"),
    31: normalize_hebrew("תצהיר יורשים"),
    32: normalize_hebrew("תצהיר יורשים - נכבדי הישוב"),
    33: normalize_hebrew("אישור יורשים - מועצת העיר"),
    34: normalize_hebrew("חשבונית"),
    35: normalize_hebrew("נסח טאבו"),
    36: normalize_hebrew("הסכמה למינוי אפוטרופוס"),
    37: normalize_hebrew("אישור יציאה לחו\"ל"),
    38: normalize_hebrew("דו\"ח סוציאלי"),
    39: normalize_hebrew("אישור מהמשטרה על הגשת תלונה"),
    40: normalize_hebrew("מסמך הודעת אישור קליטה"),
    41: normalize_hebrew("כתב מינוי מהסיוע המשפטי"),
    42: normalize_hebrew("העתק דרכון"),
    43: normalize_hebrew("בקשה לפטור מאגרה מהסיוע המשפטי"),
    44: normalize_hebrew("אישור קצבה מהביטוח הלאומי"),
    45: normalize_hebrew("תצהיר הסתלקות מהירושה"),
    46: normalize_hebrew("בקשת ביניים בתיק"),
    200: normalize_hebrew("החלטת בוררות"),
    201: normalize_hebrew("החלטת מגשרים"),
    202: normalize_hebrew("בקשה למתן ארכה"),
    203: normalize_hebrew("בקשה לדחיית הדיון"),
    204: normalize_hebrew("בקשות שונות"),
    206: normalize_hebrew("ערעור"),
    207: normalize_hebrew("דחייה"),
    208: normalize_hebrew("בקשה לתיקון החלטה/מסמך"),
    209: normalize_hebrew("מינוי בורר"),
    210: normalize_hebrew("תגובה לפי החלטת קאדי"),
    211: normalize_hebrew("בקשה לקביעת דיון"),
    212: normalize_hebrew("הקדמת מועד דיון"),
    213: normalize_hebrew("התפטרות מייצוג"),
    214: normalize_hebrew("איחוד תיקים"),
    215: normalize_hebrew("בקשה לצילום תיק"),
    216: normalize_hebrew("סגירת תיק"),
    217: normalize_hebrew("בקשה למינוי מגשרים"),
    218: normalize_hebrew("בקשות שהוגשו ע\"פ החלטת הקאדי"),
    219: normalize_hebrew("תסקיר"),
    220: normalize_hebrew("בקשה לפטור מאגרה"),
    221: normalize_hebrew("דחיית תשלום אגרה"),
    222: normalize_hebrew("החלטת פטור"),
    223: normalize_hebrew("בקשת ברית זוגיות"),
    226: normalize_hebrew("החלטה סופית לערעור"),
    277: normalize_hebrew("החלטת דחייה"),
    283: normalize_hebrew("זימון דיון"),
    869: normalize_hebrew("נימוקי הערר"),
    870: normalize_hebrew("החלטת רשות המיסים בהשגה"),
    871: normalize_hebrew("הצהרת העורר"),       
    872: normalize_hebrew("בקשה לאיסור פרסום ודיון בדלתיים סגורות"),
    873: normalize_hebrew("פרוטוקול בעלי מניות"),
    874: normalize_hebrew("בקשה להארכת מועד להגשת ערר"),
    875: normalize_hebrew("בקשה לאיחוד עררים"),
    889: normalize_hebrew("החלטה כללית"),
    890: normalize_hebrew("פרוטוקול והחלטה"),
    892: normalize_hebrew("ביטול דיון"),
    
    # Resuming from 906 onwards
    906: normalize_hebrew("בקשה לאיסור פרסום"),
    907: normalize_hebrew("בקשה לצירוף מסמכים"),
    908: normalize_hebrew("בקשה לביטול ערר"),
    909: normalize_hebrew("בקשה לקביעת / שינוי / ביטול מועד או מיקום דיון"),
    910: normalize_hebrew("בקשה לביטול / תיקון החלטה"),
    911: normalize_hebrew("בקשה לפסיקת הוצאות"),
    912: normalize_hebrew("בקשות לאחר סגירת התיק"),
    913: normalize_hebrew("אישור קבלת תגובה"),
    914: normalize_hebrew("אישור קבלת הודעה"),
    915: normalize_hebrew("אישור הוספת בקשה בתיק"),
    916: normalize_hebrew("אי תקינות מסמכים שצורפו להודעה"),
    917: normalize_hebrew("אי תקינות מסמכים שצורפו לתגובה"),
    918: normalize_hebrew("אי תקינות מסמכים שצורפו לבקשה"),
    919: normalize_hebrew("החלטת הוועדה"),
    920: normalize_hebrew("אינדיקציה למגיש"),
    921: normalize_hebrew("החלטת רשות המיסים בבקשה למענק"),
    922: normalize_hebrew("בקשה לדיון בדלתיים סגורות"),
    923: normalize_hebrew("פרוטוקול - הקלדה"),
    924: normalize_hebrew("פרוטוקול - תמליל הקלטת דיון"),
    925: normalize_hebrew("שינוי מותב"),
    926: normalize_hebrew("החלטת יחיד"),
    927: normalize_hebrew("ייפוי כוח מטעם החברה"),
    928: normalize_hebrew("פרוטוקול ישיבת דירקטוריון"),
    929: normalize_hebrew("פרוטוקול אסיפה כללית"),
    930: normalize_hebrew("פרוטוקול אסיפת שותפים"),
    931: normalize_hebrew("החלטת חברה במדינת המקור"),
    932: normalize_hebrew("החלטה בבקשה לקבלת מענק"),
    933: normalize_hebrew("דו\"ח סיכומי דיווח (ESNA) לשנת 2020"),
    934: normalize_hebrew("חשבוניות מס לקוחות"),
    935: normalize_hebrew("חוזים עם ספקים / לקוחות"),
    936: normalize_hebrew("דו\"ח על הכנסה לשנת 2020"),
    937: normalize_hebrew("דו\"ח רב שנתי לביטוח לאומי לשנת 2020 (טופס 6101)"),
    938: normalize_hebrew("דו\"ח רווח והפסד לשנת 2019"),
    939: normalize_hebrew("דו\"ח רווח והפסד לשנת 2020"),
    940: normalize_hebrew("דו\"ח מאזן בוחן לשנת 2020"),
    941: normalize_hebrew("חוזים עם לקוחות"),
    942: normalize_hebrew("תדפיסי עובר ושב בחשבון בנק"),
    943: normalize_hebrew("מסמכי ביטוח לאומי"),
    944: normalize_hebrew("בקשה להארכת מועד"),
    945: normalize_hebrew("בקשה לעדכון פרטי מייצג / עד"),
    946: normalize_hebrew("פתיחת תיק ערר"),
    947: normalize_hebrew("מוסכמת מטעם הצדדים"),
    948: normalize_hebrew("אישור קבלת ערר"),
    949: normalize_hebrew("מסמכים שנשלחו"),
    950: normalize_hebrew("בקשה להגשת ערר באופן לא מקוון"),
    951: normalize_hebrew("מסמכי ייפוי כוח והסמכה"),
    952: normalize_hebrew("מסמכי פתיחת תיק נוספים"),
    953: normalize_hebrew("פרוטוקול ישיבת ועד מנהל"),
    954: normalize_hebrew("אישור פתיחה מנהלית"),
    955: normalize_hebrew("סגירת מנהלית"),
    956: normalize_hebrew("החלטת ועדה"),
    957: normalize_hebrew("תעודת נישואין"),
    958: normalize_hebrew("שינוי מועד דיון"),
    961: normalize_hebrew("החלטה בפתקית"),
    962: normalize_hebrew("בקשה להחלפת מייצג")
}
