#!/usr/bin/env python3
"""
VIES + BCE + PEPPOL Checker — Dental Addict v4.1
- Fix lien PEPPOL directory
- NACE : libellé complet depuis la nomenclature officielle
"""

import http.server
import socketserver
import urllib.request
import urllib.error
import urllib.parse
import json
import os
import threading
import time
import re
import gzip as gz
from html.parser import HTMLParser

PORT           = int(os.environ.get("PORT", 7847))
VIES_WORKERS   = 4
VIES_DELAY     = 0.4
VIES_RETRIES   = 5
VIES_WAITS     = [1, 2, 4, 8, 16]
BCE_WORKERS    = 6
BCE_DELAY      = 0.2
PEPPOL_DELAY   = 0.1

VIES_ERRORS = {
    "MS_MAX_CONCURRENT_REQ","MS_UNAVAILABLE",
    "SERVICE_UNAVAILABLE","TIMEOUT","SERVER_BUSY",
    "MS_MAX_REQ_PER_INTERVAL_EXCEEDED"
}

_vies_sem   = threading.Semaphore(VIES_WORKERS)
_bce_sem    = threading.Semaphore(BCE_WORKERS)
_peppol_sem = threading.Semaphore(4)
_wlock      = threading.Lock()
_wtimes     = [0.0] * VIES_WORKERS
_wctr       = [0]

def _vies_slot():
    with _wlock:
        wid = _wctr[0] % VIES_WORKERS; _wctr[0] += 1
        gap = VIES_DELAY - (time.time() - _wtimes[wid])
        if gap > 0: time.sleep(gap)
        _wtimes[wid] = time.time()


# ── Dictionnaire NACE-BEL (codes fréquents secteur dentaire + santé) ──────────
# Source : NACE-BEL Rev.2 — nomenclature officielle belge
# Liste complète intégrée pour les codes les plus courants
NACE_LABELS = {
    # Santé / dentaire
    "8621": "Activités de médecine générale",
    "8622": "Activités de médecine spécialisée",
    "8623": "Pratique dentaire",
    "8690": "Autres activités pour la santé humaine",
    "8691": "Activités des centres de soins infirmiers et résidentiels",
    "8710": "Hébergement médicalisé",
    "8720": "Hébergement social pour personnes handicapées mentales, malades mentales et toxicomanes",
    "8730": "Hébergement social pour personnes âgées ou handicapées physiques",
    "8790": "Autres formes d'hébergement social",
    "8810": "Action sociale sans hébergement pour personnes âgées et handicapées",
    "8891": "Activités de soins de jour pour enfants",
    # Commerce / distribution
    "4690": "Commerce de gros non spécialisé",
    "4646": "Commerce de gros de produits pharmaceutiques",
    "4645": "Commerce de gros de parfumerie et de produits de beauté",
    "4649": "Commerce de gros d'autres biens domestiques",
    "4741": "Commerce de détail d'ordinateurs, d'unités périphériques et de logiciels",
    "4752": "Commerce de détail de quincaillerie, peintures et verres",
    "4775": "Commerce de détail de produits cosmétiques et de toilette",
    "4777": "Commerce de détail d'articles d'horlogerie et de bijouterie",
    "4779": "Commerce de détail de biens d'occasion",
    "4799": "Autres commerces de détail hors magasin, éventaires ou marchés",
    # Fabrication / industrie
    "3250": "Fabrication d'instruments et fournitures à usage médical et dentaire",
    "3251": "Fabrication de mobilier médico-chirurgical",
    "3259": "Fabrication d'autres articles médicaux et paramédicaux",
    "2110": "Fabrication de produits pharmaceutiques de base",
    "2120": "Fabrication de préparations pharmaceutiques",
    "2660": "Fabrication d'équipements d'irradiation médicale, d'équipements électriques et électroniques médicaux",
    # Services
    "6920": "Activités comptables",
    "6910": "Activités juridiques",
    "7022": "Conseil pour les affaires et autres conseils de gestion",
    "7311": "Activités des agences de publicité",
    "7320": "Études de marché et sondages d'opinion",
    "7490": "Autres activités spécialisées, scientifiques et techniques n.c.a.",
    "6201": "Programmation informatique",
    "6202": "Conseil informatique",
    "6209": "Autres activités informatiques",
    "6311": "Traitement de données, hébergement et activités connexes",
    "6312": "Portails internet",
    # Éducation / formation
    "8542": "Enseignement supérieur",
    "8559": "Autres formes d'enseignement n.c.a.",
    "8560": "Activités de soutien à l'enseignement",
    # ASBL / associations
    "9499": "Autres organisations associatives n.c.a.",
    "9430": "Organisations associatives n.c.a.",
    "9412": "Activités des organisations professionnelles",
    "9411": "Activités des organisations patronales et consulaires",
    # Immobilier
    "6810": "Activités des marchands de biens immobiliers",
    "6820": "Location et exploitation de biens immobiliers propres ou loués",
    "6831": "Agences immobilières",
    "6832": "Administration de biens immobiliers",
    # Hôtellerie / restauration
    "5510": "Hôtels et hébergement similaire",
    "5610": "Restaurants et services de restauration mobile",
    "5630": "Débits de boissons",
    # Transport / logistique
    "4941": "Transports routiers de fret",
    "5210": "Entreposage et stockage",
    "5229": "Autres services auxiliaires des transports",
    # Construction
    "4120": "Construction de bâtiments résidentiels et non résidentiels",
    "4321": "Installation électrique",
    "4322": "Travaux de plomberie et installation de chauffage et de conditionnement d'air",
    "4391": "Travaux de couverture",
    "4399": "Autres travaux de construction spécialisés n.c.a.",
    # Finance / assurance
    "6419": "Autres intermédiations monétaires",
    "6499": "Autres activités des services financiers, hors assurance et caisses de retraite n.c.a.",
    "6511": "Assurance vie",
    "6512": "Autres assurances",
}

def enrich_nace(code, libelle_bce):
    """Retourne le libellé NACE le plus complet possible."""
    if not code:
        return libelle_bce or ""
    # Chercher dans notre dictionnaire (code exact ou tronqué)
    label = NACE_LABELS.get(code)
    if not label:
        # Essai avec les 4 premiers chiffres
        label = NACE_LABELS.get(code[:4] if len(code) >= 4 else code)
    # Si on a un libellé BCE et pas de correspondance dans le dict, on garde le libellé BCE
    if not label:
        label = libelle_bce or f"Code NACE {code}"
    return label


# ── BCE Parser ────────────────────────────────────────────────
class BCEParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.d = {}
        self.nace = []
        self._th = self._td = self._h2 = False
        self._thb = self._tdb = self._h2b = ""
        self._cur_th = None
        self._h2s = []
        self._act = False; self._adep = 0
        self._arow = []; self._atd = False; self._atdb = ""

    def handle_starttag(self, tag, attrs):
        ad = dict(attrs)
        cls = ad.get("class","").lower()
        iid = ad.get("id","").lower()
        if tag=="table" and ("activit" in cls or "nace" in cls or "activit" in iid):
            self._act=True; self._adep=1
        elif tag=="table" and self._act:
            self._adep+=1
        if self._act:
            if tag=="tr": self._arow=[]
            if tag=="td": self._atd=True; self._atdb=""
        if tag=="th": self._th=True; self._thb=""
        elif tag=="td" and not self._act: self._td=True; self._tdb=""
        elif tag=="h2": self._h2=True; self._h2b=""

    def handle_endtag(self, tag):
        if tag=="table" and self._act:
            self._adep-=1
            if self._adep<=0: self._act=False
        if self._act and tag=="td":
            self._atd=False
            self._arow.append(" ".join(self._atdb.split()))
        if self._act and tag=="tr" and len(self._arow)>=2:
            c,l = self._arow[0].strip(), self._arow[1].strip()
            if c and l and c!=l and len(c)>=4 and not c.lower().startswith("cod"):
                self.nace.append((c,l))
            self._arow=[]
        if tag=="th":
            self._th=False; self._cur_th=self._thb.strip().lower()
        elif tag=="td" and not self._act:
            self._td=False
            val=" ".join(self._tdb.split()).strip()
            if self._cur_th and val:
                k=self._cur_th
                # Nom / dénomination — toutes variantes fr/nl
                if any(x in k for x in ["dénomination","denomination","naam","benaming",
                                         "raison sociale","maatschappelijke","nom officiel"]):
                    if "nom" not in self.d: self.d["nom"]=val
                # Statut
                elif any(x in k for x in ["statut","status","toestand"]):
                    self.d["statut"]=val
                # Forme juridique
                elif any(x in k for x in ["forme juridique","juridische vorm","legal form"]):
                    self.d["forme"]=val
                # Type entité
                elif any(x in k for x in ["type d","entit","entiteits","type entreprise"]):
                    if "type_entite" not in self.d: self.d["type_entite"]=val
                # Date création / début — toutes variantes
                elif any(x in k for x in [
                    "date de début","startdatum","début d","date début",
                    "date de création","création","oprichting",
                    "datum eerste","première inscription","eerste inschrijving",
                    "date d'inscription","inschrijvingsdatum"
                ]):
                    if "debut" not in self.d: self.d["debut"]=val
                # Adresse
                elif any(x in k for x in ["adresse","adres","address","siège","zetel"]) and "adresse" not in self.d:
                    self.d["adresse"]=val
                # Situation juridique
                elif any(x in k for x in ["situation juridique","juridische toestand"]):
                    self.d["situation"]=val
            self._cur_th=None
        elif tag=="h2":
            self._h2=False
            t=self._h2b.strip()
            if t and len(t)>2: self._h2s.append(t)

    def handle_data(self, s):
        if self._atd:   self._atdb+=s
        elif self._th:  self._thb+=s
        elif self._td:  self._tdb+=s
        elif self._h2:  self._h2b+=s

    def result(self):
        if "nom" not in self.d:
            for c in self._h2s:
                if not any(w in c.lower() for w in ["résultat","search","public","banque","kbo","recherche","welkom"]):
                    self.d["nom"]=c; break
        return self.d

    def get_nace(self):
        for c,l in self.nace:
            if len(c)>=4 and len(l)>5 and not c.lower().startswith("cod"):
                return {"code":c,"libelle":l}
        return {"code":"","libelle":""}

    def get_type_entite(self):
        if self.d.get("type_entite"): return self.d["type_entite"]
        if self.d.get("forme"): return self.d["forme"]
        return ""


# ── VIES ──────────────────────────────────────────────────────
def check_vies(bce):
    url = f"https://ec.europa.eu/taxation_customs/vies/rest-api/ms/BE/vat/{bce}"
    _vies_slot()
    for i in range(VIES_RETRIES):
        try:
            req = urllib.request.Request(url, headers={
                "Accept":"application/json",
                "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            })
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode())
            ue = data.get("userError","")
            if ue in VIES_ERRORS:
                w=VIES_WAITS[min(i,len(VIES_WAITS)-1)]
                print(f"  VIES {ue} BE{bce} retry {i+1} in {w}s")
                time.sleep(w); _vies_slot(); continue
            valid = data.get("isValid") is True or ue=="VALID"
            return {"ok":True,"valid":valid,
                    "name":data.get("name",""),
                    "address":data.get("address",""),
                    "requestDate":data.get("requestDate",""),
                    "error":ue if not valid and ue not in ("","VALID") else ""}
        except urllib.error.HTTPError as e:
            if e.code in (429,503,504):
                w=VIES_WAITS[min(i,len(VIES_WAITS)-1)]
                time.sleep(w); _vies_slot(); continue
            return {"ok":False,"error":f"HTTP {e.code}"}
        except Exception as e:
            if i<VIES_RETRIES-1:
                time.sleep(VIES_WAITS[min(i,len(VIES_WAITS)-1)]); continue
            return {"ok":False,"error":str(e)[:100]}
    return {"ok":False,"error":f"Echec {VIES_RETRIES} tentatives"}


# ── BCE ───────────────────────────────────────────────────────
def check_bce(bce):
    url = f"https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html?nummer={bce}&actionLu=Recherche&lang=fr"
    time.sleep(BCE_DELAY)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept":"text/html,application/xhtml+xml",
            "Accept-Language":"fr-FR,fr;q=0.95,nl;q=0.5"
        })
        with urllib.request.urlopen(req, timeout=12) as r:
            raw=r.read()
        try:    html=gz.decompress(raw).decode("utf-8",errors="replace")
        except: html=raw.decode("utf-8",errors="replace")

        p=BCEParser(); p.feed(html)
        d=p.result(); nace_raw=p.get_nace()
        te=p.get_type_entite()

        # Fallback nom : schema:legalName ou og:title ou h1
        if not d.get("nom"):
            m = re.search(r'property="schema:legalName"[^>]*>([^<]{3,100})', html)
            if m: d["nom"] = m.group(1).strip()
        if not d.get("nom"):
            m = re.search(r'<h1[^>]*>([^<]{3,100})</h1>', html)
            if m:
                t = m.group(1).strip()
                if not any(w in t.lower() for w in ["kbo","bce","public","search","résultat"]):
                    d["nom"] = t
        if not d.get("nom"):
            # Chercher dans les meta
            m = re.search(r'<title>([^<|–-]{3,80})', html)
            if m:
                t = m.group(1).strip()
                if not any(w in t.lower() for w in ["kbo","bce","public","search"]):
                    d["nom"] = t

        # Fallback date création : patterns directs dans le HTML
        if not d.get("debut"):
            # Format DD-MM-YYYY ou DD/MM/YYYY après mots-clés
            m = re.search(
                r'(?:date\s+de\s+d[eé]but|startdatum|datum\s+eerste|cr[eé]ation|oprichting|inschrijving)[^<]{0,30}?(\d{2}[-/]\d{2}[-/]\d{4})',
                html, re.I)
            if m: d["debut"] = m.group(1).replace("/","-")
        if not d.get("debut"):
            # Chercher dans les tableaux : ligne contenant une date ISO ou belge
            m = re.search(
                r'<td[^>]*>\s*(\d{2}[-/]\d{2}[-/]\d{4})\s*</td>',
                html)
            if m: d["debut"] = m.group(1).replace("/","-")

        # Fallback NACE regex si le parser n'a rien trouvé
        if not nace_raw["code"]:
            m=re.search(r'(\d{4,5})\s*[-–]\s*([A-ZÀ-Ÿa-zà-ÿ][^<\n]{10,80})',html)
            if m:
                nace_raw={"code":m.group(1),"libelle":m.group(2).strip().rstrip(".,;")}

        # Fallback nom : chercher dans les balises title ou h1 si parser n'a rien trouvé
        if not d.get("nom"):
            # Essai sur le titre de page
            m=re.search(r'<title[^>]*>([^<]{3,80})</title>',html,re.I)
            if m:
                t=m.group(1).strip()
                if t and not any(w in t.lower() for w in ["kbo","bce","public","search","zoek"]):
                    d["nom"]=t
            # Essai sur h1
            if not d.get("nom"):
                m=re.search(r'<h1[^>]*>([^<]{3,80})</h1>',html,re.I)
                if m:
                    t=m.group(1).strip()
                    if t and len(t)>3: d["nom"]=t

        # Fallback date création regex
        if not d.get("debut"):
            m=re.search(r'(?:date\s+de\s+(?:d[ée]but|cr[ée]ation|fondation)|oprichtingsdatum|startdatum)[^:]*:\s*([0-9]{1,2}[\./][0-9]{1,2}[\./][0-9]{2,4})',html,re.I)
            if m: d["debut"]=m.group(1).strip()
            if not d.get("debut"):
                # Format ISO dans attribut data ou span
                m=re.search(r"(?:creation|oprichting|fondation)[^\"']{0,30}[\"']([0-9]{4}-[0-9]{2}-[0-9]{2})[\"']",html,re.I)
                if m: d["debut"]=m.group(1)

        # Fallback type entité regex
        if not te:
            m=re.search(r'(?:type\s+d.entit[eé]|entiteitstype)[^:]*:\s*([^\n<]{3,60})',html,re.I)
            if m: te=m.group(1).strip()

        # Enrichir le libellé NACE
        nace_code    = nace_raw.get("code","")
        nace_libelle = enrich_nace(nace_code, nace_raw.get("libelle",""))

        body=html.lower()
        not_found=any(x in body for x in ["aucune entreprise","geen onderneming","no enterprise"])
        found=not not_found and (d.get("nom") or bce[:6] in html)

        return {
            "ok":True,"found":found,
            "nom":d.get("nom",""),
            "statut":d.get("statut","Actif" if found else ""),
            "type_entite":te,
            "forme":d.get("forme",""),
            "debut":d.get("debut",""),
            "date_inscription":d.get("date_inscription",""),
            "adresse":d.get("adresse",""),
            "situation":d.get("situation",""),
            "nace_code":nace_code,
            "nace_libelle":nace_libelle
        }
    except urllib.error.HTTPError as e:
        return {"ok":False,"error":f"BCE HTTP {e.code}"}
    except Exception as e:
        return {"ok":False,"error":str(e)[:100]}


# ── PEPPOL ────────────────────────────────────────────────────
def check_peppol(bce):
    time.sleep(PEPPOL_DELAY)
    participant     = f"iso6523-actorid-upis::0208:{bce}"
    participant_enc = urllib.parse.quote(participant, safe="")

    # 1. SMP belge officiel (source de vérité, HTTP public sans auth)
    smp_url = f"http://smp.belgium.be/{participant_enc}"
    try:
        req = urllib.request.Request(smp_url, headers={
            "Accept":"application/xml,*/*",
            "User-Agent":"Mozilla/5.0"
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read().decode("utf-8",errors="replace")
        registered = "<ParticipantIdentifier" in body or "<smp:" in body or "0208:" in body
        doc_types = []
        if registered:
            if "Invoice" in body:    doc_types.append("Facture (BIS Billing 3)")
            if "CreditNote" in body: doc_types.append("Note de crédit")
            if "Order" in body:      doc_types.append("Commande")
            if not doc_types:        doc_types.append("Documents électroniques")

        # Lien direct vers la fiche dans le répertoire PEPPOL (URL corrigée)
        dir_url = f"https://directory.peppol.eu/public/locale-en_US/menuitem-search?q=0208%3A{bce}"

        return {
            "ok":True,
            "registered":registered,
            "peppol_id": f"0208:{bce}" if registered else "",
            "doc_types":doc_types,
            "source":"SMP Belgium (officiel)",
            "dir_url": dir_url
        }
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # 404 = pas sur le SMP belge → fallback répertoire global
            return _peppol_directory(bce)
        return {"ok":True,"registered":False,"peppol_id":"","doc_types":[],"source":"SMP Belgium","dir_url":""}
    except Exception:
        return _peppol_directory(bce)


def _peppol_directory(bce):
    """Fallback : répertoire PEPPOL global via API JSON publique."""
    # URL corrigée de l'API JSON du répertoire PEPPOL
    url = f"https://directory.peppol.eu/search/1.0/json?q=0208%3A{bce}&rpc=0208&country=BE"
    dir_url = f"https://directory.peppol.eu/public/locale-en_US/menuitem-search?q=0208%3A{bce}"
    try:
        req = urllib.request.Request(url, headers={
            "Accept":"application/json",
            "User-Agent":"Mozilla/5.0"
        })
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode())
        matches = data.get("matches",[])
        if matches:
            m   = matches[0]
            pid = m.get("participantID",{}).get("value","")
            return {
                "ok":True,"registered":True,
                "peppol_id":pid or f"0208:{bce}",
                "doc_types":["Voir répertoire PEPPOL"],
                "source":"Répertoire PEPPOL",
                "dir_url": dir_url
            }
        return {"ok":True,"registered":False,"peppol_id":"","doc_types":[],"source":"Répertoire PEPPOL","dir_url":dir_url}
    except Exception as e:
        return {"ok":False,"error":str(e)[:80],"registered":False,"peppol_id":"","doc_types":[],"dir_url":dir_url}



# ── BCE Recherche par nom ─────────────────────────────────────
class BCESearchParser(HTMLParser):
    """Parse les résultats de recherche par nom sur BCE Public Search."""
    def __init__(self):
        super().__init__()
        self.results = []
        self._in_table = False
        self._in_row   = False
        self._in_cell  = False
        self._cells    = []
        self._cell_buf = ""
        self._row_link = ""
        self._in_a     = False
        self._a_href   = ""
        self._depth    = 0

    def handle_starttag(self, tag, attrs):
        ad = dict(attrs)
        if tag == "table":
            cls = ad.get("class","").lower()
            if "table" in cls or "result" in cls or "zoek" in cls:
                self._in_table = True; self._depth = 1
            elif self._in_table:
                self._depth += 1
        if self._in_table and tag == "tr":
            self._in_row = True; self._cells = []; self._row_link = ""
        if self._in_table and self._in_row and tag in ("td","th"):
            self._in_cell = True; self._cell_buf = ""
        if self._in_table and self._in_row and tag == "a":
            self._in_a = True; self._a_href = ad.get("href","")

    def handle_endtag(self, tag):
        if tag == "table" and self._in_table:
            self._depth -= 1
            if self._depth <= 0: self._in_table = False
        if self._in_table and self._in_row and tag in ("td","th"):
            self._in_cell = False
            self._cells.append((" ".join(self._cell_buf.split()), self._row_link))
            self._row_link = ""
        if self._in_table and tag == "tr" and self._in_row:
            self._in_row = False
            # Extraire numéro BCE et nom depuis les cellules
            texts = [c[0] for c in self._cells]
            links = [c[1] for c in self._cells]
            # Chercher un numéro BCE (pattern 0XXX.XXX.XXX ou 10 chiffres)
            bce = ""; nom = ""; statut = ""
            for i, t in enumerate(texts):
                m = re.search(r'(\d{4}\.\d{3}\.\d{3}|\d{10})', t)
                if m:
                    bce = m.group(1).replace(".","")
                    # Le nom est souvent dans la cellule précédente ou suivante
                    if i > 0 and texts[i-1] and not re.search(r'\d{4}', texts[i-1]):
                        nom = texts[i-1]
                    elif i < len(texts)-1 and texts[i+1]:
                        nom = texts[i+1]
                if "actif" in t.lower() or "actief" in t.lower():
                    statut = "Actif"
                elif "arrêt" in t.lower() or "gestopt" in t.lower():
                    statut = "Arrêté"
            # Fallback : toute la ligne comme nom si pas de numéro trouvé séparément
            if bce and not nom:
                nom = " — ".join(t for t in texts if t and not re.search(r'\d{4}\.\d{3}', t) and len(t) > 1)
            if bce and len(bce) == 10:
                self.results.append({
                    "bce": bce,
                    "nom": nom[:80] if nom else "",
                    "statut": statut
                })

    def handle_data(self, s):
        if self._in_cell:
            self._cell_buf += s
        if self._in_a and self._a_href:
            self._row_link = self._a_href
            self._in_a = False



def _debug_search(query):
    """Endpoint de debug — retourne le HTML brut + analyse pour diagnostiquer le parser."""
    import traceback
    results = {}

    # ── Test POST phonétique ──────────────────────────────────
    try:
        post_data = urllib.parse.urlencode({
            "searchWord": query, "_activeq": "on",
            "actionLu": "Zoek", "lang": "fr"
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://kbopub.economie.fgov.be/kbopub/zoeknaamfonetischform.html",
            data=post_data,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "fr-FR,fr;q=0.95",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://kbopub.economie.fgov.be/kbopub/zoeknaamfonetischform.html?lang=fr"
            }
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")

        # Analyse
        bce_nums  = re.findall(r'\d{4}[.\s]\d{3}[.\s]\d{3}', html)
        bce_links = re.findall(r'href="([^"]*zoeknummerform[^"]*nummer=\d+[^"]*)"', html, re.I)
        tables    = re.findall(r'<table[^>]*class="([^"]*)"', html, re.I)
        trs       = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.I)
        # Extraire quelques lignes de tableau pour voir la structure
        sample_rows = []
        for tr in trs[:10]:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL | re.I)
            clean = [re.sub(r'<[^>]+>', '', td).strip() for td in tds]
            clean = [c for c in clean if c]
            if clean:
                sample_rows.append(clean)

        results["post"] = {
            "ok": True,
            "html_length": len(html),
            "bce_numbers_found": bce_nums[:10],
            "bce_links_found": bce_links[:5],
            "table_classes": tables[:5],
            "sample_rows": sample_rows[:8],
            "html_snippet": html[2000:4000]  # milieu du HTML souvent = résultats
        }
    except Exception as e:
        results["post"] = {"ok": False, "error": str(e), "trace": traceback.format_exc()[-300:]}

    # ── Test GET (certains serveurs acceptent les deux) ───────
    try:
        get_url = ("https://kbopub.economie.fgov.be/kbopub/zoeknaamfonetischform.html?"
                   + urllib.parse.urlencode({"searchWord": query, "_activeq": "on",
                                             "actionLu": "Zoek", "lang": "fr"}))
        req2 = urllib.request.Request(get_url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept": "text/html",
        })
        with urllib.request.urlopen(req2, timeout=12) as r:
            html2 = r.read().decode("utf-8", errors="replace")
        bce2 = re.findall(r'\d{4}[.\s]\d{3}[.\s]\d{3}', html2)
        results["get"] = {
            "ok": True,
            "html_length": len(html2),
            "bce_numbers_found": bce2[:10],
            "html_snippet": html2[2000:3500]
        }
    except Exception as e:
        results["get"] = {"ok": False, "error": str(e)}

    # ── Test version mobile ───────────────────────────────────
    try:
        mob_url = ("https://kbopub.economie.fgov.be/kbopub-m/zoeknamepage?"
                   + urllib.parse.urlencode({"searchWord": query, "actief": "true", "lang": "fr"}))
        req3 = urllib.request.Request(mob_url, headers={
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0)",
            "Accept": "text/html",
        })
        with urllib.request.urlopen(req3, timeout=12) as r:
            html3 = r.read().decode("utf-8", errors="replace")
        bce3 = re.findall(r'\d{4}[.\s]\d{3}[.\s]\d{3}', html3)
        trs3 = re.findall(r'<tr[^>]*>(.*?)</tr>', html3, re.DOTALL | re.I)
        rows3 = []
        for tr in trs3[:10]:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', tr, re.DOTALL | re.I)
            clean = [re.sub(r'<[^>]+>', '', td).strip() for td in tds]
            clean = [c for c in clean if c]
            if clean: rows3.append(clean)
        results["mobile"] = {
            "ok": True,
            "html_length": len(html3),
            "bce_numbers_found": bce3[:10],
            "sample_rows": rows3[:8],
            "html_snippet": html3[:3000]
        }
    except Exception as e:
        results["mobile"] = {"ok": False, "error": str(e)}

    return {"query": query, "results": results}

def search_bce_by_name(query):
    """
    Recherche par nom via BCE Public Search (POST form).
    Utilise aussi la version mobile en fallback — structure HTML plus simple.
    """
    results = []
    query_clean = query.strip()

    # ── Méthode 1 : POST vers le formulaire phonétique principal ──────
    try:
        post_data = urllib.parse.urlencode({
            "searchWord": query_clean,
            "_activeq": "on",
            "actionLu": "Zoek",
            "lang": "fr"
        }).encode("utf-8")

        req = urllib.request.Request(
            "https://kbopub.economie.fgov.be/kbopub/zoeknaamfonetischform.html",
            data=post_data,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "fr-FR,fr;q=0.95",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://kbopub.economie.fgov.be/kbopub/zoeknaamfonetischform.html?lang=fr"
            }
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            raw  = r.read()
            html = raw.decode("utf-8", errors="replace")

        results = _parse_bce_search_results(html, query_clean)

    except Exception as e:
        print(f"  BCE search POST error: {e}")

    # ── Méthode 2 : version mobile (fallback, HTML plus simple) ───────
    if not results:
        try:
            mob_url = "https://kbopub.economie.fgov.be/kbopub-m/zoeknamepage?" + urllib.parse.urlencode({
                "searchWord": query_clean,
                "actief": "true",
                "lang": "fr"
            })
            req2 = urllib.request.Request(mob_url, headers={
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
                "Accept": "text/html",
                "Accept-Language": "fr-FR,fr;q=0.9"
            })
            with urllib.request.urlopen(req2, timeout=12) as r:
                html2 = r.read().decode("utf-8", errors="replace")
            results = _parse_bce_mobile_results(html2)
        except Exception as e:
            print(f"  BCE mobile search error: {e}")

    # Dédoublonner
    seen = set()
    unique = []
    for r in results:
        if r["bce"] and r["bce"] not in seen and len(r["bce"]) == 10:
            seen.add(r["bce"])
            unique.append(r)

    return {"ok": True, "results": unique[:30], "query": query_clean}


def _parse_bce_search_results(html, query):
    """Parse la page de résultats BCE Public Search standard."""
    results = []
    # Les résultats sont dans un tableau avec des liens vers les fiches
    # Pattern : lien vers zoeknummerform avec le numéro, + nom dans le texte
    
    # Chercher tous les liens vers des fiches entreprise
    link_pattern = re.compile(
        r'href="[^"]*zoeknummerform[^"]*nummer=(\d+)[^"]*"[^>]*>([^<]+)</a>',
        re.IGNORECASE
    )
    for m in link_pattern.finditer(html):
        bce_raw = m.group(1).strip()
        nom     = m.group(2).strip()
        bce = bce_raw.replace(".", "")
        if len(bce) == 10 and nom and len(nom) > 1:
            # Déterminer le statut depuis le contexte autour du lien
            ctx = html[max(0, m.start()-200):m.end()+200].lower()
            if "actif" in ctx or "actief" in ctx:
                statut = "Actif"
            elif "arrêt" in ctx or "gestopt" in ctx or "stop" in ctx:
                statut = "Arrêté"
            else:
                statut = ""
            results.append({"bce": bce, "nom": nom[:80], "statut": statut})

    # Fallback : regex sur les numéros BCE dans le HTML
    if not results:
        num_pattern = re.compile(r'(\d{4})\.(\d{3})\.(\d{3})')
        for m in num_pattern.finditer(html):
            bce = m.group(1) + m.group(2) + m.group(3)
            # Chercher le nom autour
            ctx   = html[max(0, m.start()-300):m.end()+300]
            nom_m = re.search(r'<td[^>]*>\s*<a[^>]*>([^<]{3,60})</a>', ctx)
            nom   = nom_m.group(1).strip() if nom_m else ""
            results.append({"bce": bce, "nom": nom or f"Entreprise {bce}", "statut": ""})

    return results


def _parse_bce_mobile_results(html):
    """Parse la version mobile BCE — HTML plus simple et structuré."""
    results = []
    # La version mobile utilise des li ou div avec classe "listitem" ou similaire
    # Pattern commun : numéro + nom + statut
    
    # Chercher les entrées de liste
    item_pattern = re.compile(
        r'(\d{4}[\. ]\d{3}[\. ]\d{3})[^\w]*([A-Za-z][^<]{2,60})',
        re.UNICODE
    )
    for m in item_pattern.finditer(html):
        bce = re.sub(r'[^\d]', '', m.group(1))
        nom = m.group(2).strip().rstrip(" -|/")
        if len(bce) == 10 and nom and len(nom) > 2:
            ctx    = html[max(0, m.start()-100):m.end()+100].lower()
            statut = "Actif" if "actif" in ctx else "Arrêté" if "arrêt" in ctx else ""
            results.append({"bce": bce, "nom": nom[:80], "statut": statut})

    return results

# ── Normalisation + check complet ────────────────────────────
def normalize(raw):
    v=raw.strip().upper().replace(".","").replace(" ","").replace("-","")
    if v.startswith("BE"): v=v[2:]
    v="".join(c for c in v if c.isdigit())
    if len(v)==9: v="0"+v
    return v

def check_all(bce):
    if len(bce)!=10:
        e={"ok":False,"error":"Format invalide (≠10 chiffres)"}
        return {"bce":bce,"vies":e,"bceData":e,"peppol":e}
    res={}
    def dv():
        with _vies_sem: res["vies"]=check_vies(bce)
    def db():
        with _bce_sem:  res["bce"] =check_bce(bce)
    def dp():
        with _peppol_sem: res["peppol"]=check_peppol(bce)
    ts=[threading.Thread(target=f) for f in (dv,db,dp)]
    for t in ts: t.start()
    for t in ts: t.join()
    return {"bce":bce,"vies":res.get("vies"),"bceData":res.get("bce"),"peppol":res.get("peppol")}


# ── HTTP Handler ──────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,fmt,*a): pass

    def do_OPTIONS(self):
        self.send_response(200); self._cors(); self.end_headers()

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers","Content-Type")

    def do_GET(self):
        if self.path in ("/","/index.html"):
            self._serve_html()
        elif self.path.startswith("/check?"):
            p=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            bce=normalize(p.get("bce",[""])[0])
            print(f"  /check BE{bce}")
            self._json(check_all(bce))
        elif self.path.startswith("/search?"):
            p=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            q=p.get("q",[""])[0].strip()
            if q:
                print(f"  /search {q!r}")
                self._json(search_bce_by_name(q))
            else:
                self._json({"ok":False,"error":"Paramètre q manquant","results":[]})
        elif self.path=="/health":
            self._json({"status":"ok","version":"4.2"})
        elif self.path.startswith("/debug-search?"):
            p   = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            q   = p.get("q",["Henrion"])[0].strip()
            self._json(_debug_search(q))
        else:
            self.send_response(404); self.end_headers()

    def do_POST(self):
        if self.path!="/batch": return
        body=json.loads(self.rfile.read(int(self.headers.get("Content-Length",0))).decode())
        numbers=body.get("numbers",[])
        total=len(numbers)
        print(f"\n  Batch {total} nums — VIES×{VIES_WORKERS} BCE×{BCE_WORKERS} PEPPOL×4")
        results=[None]*total
        lock=threading.Lock(); done=[0]
        def process(idx,raw):
            bce=normalize(raw); r=check_all(bce)
            with lock:
                results[idx]=r; done[0]+=1
                pct=done[0]*100//total
                print(f"\r  [{'='*(pct//5)}{' '*(20-pct//5)}] {done[0]}/{total} ({pct}%) BE{bce}",end="",flush=True)
        pool=[]
        for i,raw in enumerate(numbers):
            while threading.active_count()>VIES_WORKERS*3+20: time.sleep(0.05)
            t=threading.Thread(target=process,args=(i,raw),daemon=True)
            t.start(); pool.append(t)
            time.sleep(VIES_DELAY/VIES_WORKERS)
        for t in pool: t.join()
        print(f"\n  OK {total}\n")
        self._json(results)

    def _serve_html(self):
        p=os.path.join(os.path.dirname(os.path.abspath(__file__)),"app.html")
        if not os.path.exists(p):
            self.send_response(404); self.end_headers()
            self.wfile.write(b"app.html introuvable"); return
        with open(p,"rb") as f: content=f.read()
        self.send_response(200)
        self.send_header("Content-Type","text/html; charset=utf-8")
        self.send_header("Content-Length",str(len(content)))
        self._cors(); self.end_headers(); self.wfile.write(content)

    def _json(self,data):
        pl=json.dumps(data,ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(pl)))
        self._cors(); self.end_headers(); self.wfile.write(pl)


class ThreadedServer(socketserver.ThreadingMixIn,socketserver.TCPServer):
    allow_reuse_address=True; daemon_threads=True


def main():
    is_cloud = os.environ.get("RENDER") or os.environ.get("RAILWAY_ENVIRONMENT")
    print("="*60)
    print("  VIES + BCE + PEPPOL Checker — Dental Addict v4.1")
    print("="*60)
    print(f"  Port : {PORT}  {'[Cloud]' if is_cloud else '[Local]'}")
    print("="*60)
    bind = "0.0.0.0" if is_cloud else "localhost"
    with ThreadedServer((bind,PORT),Handler) as srv:
        if not is_cloud:
            import webbrowser
            threading.Thread(target=lambda:(time.sleep(1.2),webbrowser.open(f"http://localhost:{PORT}")),daemon=True).start()
        try: srv.serve_forever()
        except KeyboardInterrupt: print("\n  Arret.")

if __name__=="__main__":
    main()
