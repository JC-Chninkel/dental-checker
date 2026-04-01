#!/usr/bin/env python3
"""VIES + BCE + PEPPOL Checker — Dental Addict v5"""

import http.server, socketserver, urllib.request, urllib.error, urllib.parse
import json, os, threading, time, re, gzip as gz
from html.parser import HTMLParser

PORT         = int(os.environ.get("PORT", 7847))
VIES_WORKERS = 4; VIES_DELAY = 0.4; VIES_RETRIES = 5
VIES_WAITS   = [1, 2, 4, 8, 16]
BCE_WORKERS  = 6; BCE_DELAY  = 0.2; PEPPOL_DELAY = 0.1
VIES_ERRORS  = {"MS_MAX_CONCURRENT_REQ","MS_UNAVAILABLE","SERVICE_UNAVAILABLE","TIMEOUT","SERVER_BUSY","MS_MAX_REQ_PER_INTERVAL_EXCEEDED"}
_vies_sem    = threading.Semaphore(VIES_WORKERS)
_bce_sem     = threading.Semaphore(BCE_WORKERS)
_peppol_sem  = threading.Semaphore(4)
_wlock       = threading.Lock()
_wtimes      = [0.0]*VIES_WORKERS; _wctr = [0]

def _vies_slot():
    with _wlock:
        wid = _wctr[0]%VIES_WORKERS; _wctr[0]+=1
        gap = VIES_DELAY-(time.time()-_wtimes[wid])
        if gap>0: time.sleep(gap)
        _wtimes[wid]=time.time()

NACE_LABELS = {
    "8621":"Activités de médecine générale","8622":"Activités de médecine spécialisée",
    "8623":"Pratique dentaire","8690":"Autres activités pour la santé humaine",
    "8710":"Hébergement médicalisé","8730":"Hébergement social pour personnes âgées",
    "4690":"Commerce de gros non spécialisé","4646":"Commerce de gros produits pharmaceutiques",
    "4645":"Commerce de gros parfumerie et beauté","4649":"Commerce de gros autres biens domestiques",
    "3250":"Fabrication instruments usage médical et dentaire","2120":"Fabrication préparations pharmaceutiques",
    "2660":"Fabrication équipements médicaux électroniques","6920":"Activités comptables",
    "6910":"Activités juridiques","7022":"Conseil affaires et gestion","7490":"Autres activités spécialisées",
    "6201":"Programmation informatique","6202":"Conseil informatique","8542":"Enseignement supérieur",
    "8559":"Autres formes d'enseignement","9499":"Autres organisations associatives",
    "9412":"Activités des organisations professionnelles","6820":"Location biens immobiliers",
    "6831":"Agences immobilières","4941":"Transports routiers de fret",
    "4120":"Construction bâtiments résidentiels et non résidentiels",
}

def enrich_nace(code, libelle):
    if not code: return libelle or ""
    return NACE_LABELS.get(code) or NACE_LABELS.get(code[:4] if len(code)>=4 else code) or libelle or f"Code NACE {code}"

class BCEParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.d={}; self.nace=[]
        self._th=self._td=self._h2=False
        self._thb=self._tdb=self._h2b=""
        self._cur_th=None; self._h2s=[]
        self._act=False; self._adep=0
        self._arow=[]; self._atd=False; self._atdb=""

    def handle_starttag(self, tag, attrs):
        ad=dict(attrs); cls=ad.get("class","").lower(); iid=ad.get("id","").lower()
        if tag=="table" and ("activit" in cls or "nace" in cls or "activit" in iid): self._act=True; self._adep=1
        elif tag=="table" and self._act: self._adep+=1
        if self._act:
            if tag=="tr": self._arow=[]
            if tag=="td": self._atd=True; self._atdb=""
        if   tag=="th":                   self._th=True;  self._thb=""
        elif tag=="td" and not self._act: self._td=True;  self._tdb=""
        elif tag=="h2":                   self._h2=True;  self._h2b=""

    def handle_endtag(self, tag):
        if tag=="table" and self._act:
            self._adep-=1
            if self._adep<=0: self._act=False
        if self._act and tag=="td":
            self._atd=False; self._arow.append(" ".join(self._atdb.split()))
        if self._act and tag=="tr" and len(self._arow)>=2:
            c,l=self._arow[0].strip(),self._arow[1].strip()
            if c and l and c!=l and len(c)>=4 and not c.lower().startswith("cod"): self.nace.append((c,l))
            self._arow=[]
        if tag=="th":
            self._th=False; self._cur_th=self._thb.strip().lower()
        elif tag=="td" and not self._act:
            self._td=False; val=" ".join(self._tdb.split())
            if self._cur_th:
                k=self._cur_th
                if any(x in k for x in ["dénomination","denomination","naam","benaming","raison"]):
                    if "nom" not in self.d: self.d["nom"]=val
                elif any(x in k for x in ["statut","status","toestand"]): self.d["statut"]=val
                elif any(x in k for x in ["forme juridique","juridische vorm"]): self.d["forme"]=val
                elif any(x in k for x in ["type d'entité","type entit","entiteitstype"]): self.d["type_entite"]=val
                elif any(x in k for x in ["date de début","startdatum","début d","création","fondation"]):
                    if "debut" not in self.d: self.d["debut"]=val
                elif any(x in k for x in ["date d'inscription","inschrijvingsdatum"]):
                    if "date_inscription" not in self.d: self.d["date_inscription"]=val
                elif any(x in k for x in ["adresse","adres","address"]) and "adresse" not in self.d: self.d["adresse"]=val
                elif any(x in k for x in ["situation juridique","juridische toestand"]): self.d["situation"]=val
                elif any(x in k for x in ["téléphone","telefoon","phone"]): self.d["telephone"]=val
                elif any(x in k for x in ["email","e-mail","courriel"]): self.d["email"]=val
                elif any(x in k for x in ["site web","website","url"]): self.d["website"]=val
                elif any(x in k for x in ["nombre d'unité","aantal vest","etablissement"]): self.d["nb_etablissements"]=val
            self._cur_th=None
        elif tag=="h2":
            self._h2=False; t=self._h2b.strip()
            if t and len(t)>2: self._h2s.append(t)

    def handle_data(self,s):
        if   self._atd: self._atdb+=s
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
            if len(c)>=4 and len(l)>5 and not c.lower().startswith("cod"): return {"code":c,"libelle":l}
        return {"code":"","libelle":""}

    def get_type_entite(self): return self.d.get("type_entite") or self.d.get("forme") or ""

def check_vies(bce):
    url="https://ec.europa.eu/taxation_customs/vies/rest-api/ms/BE/vat/"+bce
    _vies_slot()
    for i in range(VIES_RETRIES):
        try:
            req=urllib.request.Request(url,headers={"Accept":"application/json","User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"})
            with urllib.request.urlopen(req,timeout=15) as r: data=json.loads(r.read().decode())
            ue=data.get("userError","")
            if ue in VIES_ERRORS:
                w=VIES_WAITS[min(i,len(VIES_WAITS)-1)]; print(f"  VIES {ue} BE{bce} retry {i+1} in {w}s"); time.sleep(w); _vies_slot(); continue
            valid=data.get("isValid") is True or ue=="VALID"
            return {"ok":True,"valid":valid,"name":data.get("name",""),"address":data.get("address",""),"requestDate":data.get("requestDate",""),"error":ue if not valid and ue not in ("","VALID") else ""}
        except urllib.error.HTTPError as e:
            if e.code in (429,503,504): w=VIES_WAITS[min(i,len(VIES_WAITS)-1)]; time.sleep(w); _vies_slot(); continue
            return {"ok":False,"error":f"HTTP {e.code}"}
        except Exception as e:
            if i<VIES_RETRIES-1: time.sleep(VIES_WAITS[min(i,len(VIES_WAITS)-1)]); continue
            return {"ok":False,"error":str(e)[:100]}
    return {"ok":False,"error":f"Echec {VIES_RETRIES} tentatives"}

def check_bce(bce):
    url=f"https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html?nummer={bce}&actionLu=Recherche&lang=fr"
    time.sleep(BCE_DELAY)
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)","Accept":"text/html,application/xhtml+xml","Accept-Language":"fr-FR,fr;q=0.95,nl;q=0.5"})
        with urllib.request.urlopen(req,timeout=12) as r: raw=r.read()
        try:    html=gz.decompress(raw).decode("utf-8",errors="replace")
        except: html=raw.decode("utf-8",errors="replace")

        p=BCEParser(); p.feed(html); d=p.result(); nace_raw=p.get_nace(); te=p.get_type_entite()

        if not d.get("nom"):
            m=re.search(r'<title[^>]*>([^<]{3,80})</title>',html,re.I)
            if m:
                t=m.group(1).strip()
                if t and not any(w in t.lower() for w in ["kbo","bce","public","search","zoek"]): d["nom"]=t

        if not d.get("debut"):
            m=re.search(r'(?:date\s+de\s+(?:d[ée]but|cr[ée]ation|fondation)|startdatum)[^:]*:\s*([0-9]{1,2}[./][0-9]{1,2}[./][0-9]{2,4})',html,re.I)
            if m: d["debut"]=m.group(1).strip()

        if not te:
            m=re.search(r'(?:type\s+d.entit[eé]|entiteitstype)[^:]*:\s*([^\n<]{3,60})',html,re.I)
            if m: te=m.group(1).strip()

        if not d.get("telephone"):
            m=re.search(r'(?:tel|phone)[^:]*:\s*([\d\s+\-.]{8,20})',html,re.I)
            if m: d["telephone"]=m.group(1).strip()

        if not d.get("email"):
            m=re.search(r'[\w.\-]+@[\w.\-]+\.[a-z]{2,6}',html,re.I)
            if m: d["email"]=m.group(0)

        if not d.get("website"):
            m=re.search(r'https?://(?!kbopub|economie\.fgov|belgium\.be|myenterprise)[\w.\-/]+\.[a-z]{2,6}[\w/\-?=&%]*',html,re.I)
            if m: d["website"]=m.group(0)[:80]

        nace_code=nace_raw.get("code",""); nace_libelle=enrich_nace(nace_code,nace_raw.get("libelle",""))
        body=html.lower(); not_found=any(x in body for x in ["aucune entreprise","geen onderneming","no enterprise"])
        found=not not_found and (d.get("nom") or bce[:6] in html)

        return {"ok":True,"found":found,
                "nom":d.get("nom",""),"statut":d.get("statut","Actif" if found else ""),
                "type_entite":te,"forme":d.get("forme",""),
                "debut":d.get("debut",""),"date_inscription":d.get("date_inscription",""),
                "adresse":d.get("adresse",""),"situation":d.get("situation",""),
                "telephone":d.get("telephone",""),"email":d.get("email",""),"website":d.get("website",""),
                "nb_etablissements":d.get("nb_etablissements",""),
                "nace_code":nace_code,"nace_libelle":nace_libelle}
    except urllib.error.HTTPError as e: return {"ok":False,"error":f"BCE HTTP {e.code}"}
    except Exception as e: return {"ok":False,"error":str(e)[:100]}

def check_peppol(bce):
    time.sleep(PEPPOL_DELAY)
    participant=f"iso6523-actorid-upis::0208:{bce}"
    participant_enc=urllib.parse.quote(participant,safe="")
    dir_url=f"https://directory.peppol.eu/public/locale-en_US/menuitem-search?q=0208%3A{bce}"
    try:
        req=urllib.request.Request(f"http://smp.belgium.be/{participant_enc}",headers={"Accept":"application/xml,*/*","User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req,timeout=10) as r: body=r.read().decode("utf-8",errors="replace")
        registered="<ParticipantIdentifier" in body or "<smp:" in body or "0208:" in body
        doc_types=[]
        if registered:
            if "Invoice"    in body: doc_types.append("Facture (BIS Billing 3)")
            if "CreditNote" in body: doc_types.append("Note de crédit")
            if "Order"      in body: doc_types.append("Commande")
            if not doc_types:        doc_types.append("Documents électroniques")
        return {"ok":True,"registered":registered,"peppol_id":f"0208:{bce}" if registered else "","doc_types":doc_types,"source":"SMP Belgium","dir_url":dir_url}
    except urllib.error.HTTPError as e:
        return {"ok":True,"registered":False,"peppol_id":"","doc_types":[],"source":"SMP Belgium","dir_url":dir_url}
    except Exception:
        try:
            url2=f"https://directory.peppol.eu/search/1.0/json?q=0208%3A{bce}&country=BE"
            req2=urllib.request.Request(url2,headers={"Accept":"application/json","User-Agent":"Mozilla/5.0"})
            with urllib.request.urlopen(req2,timeout=8) as r: data2=json.loads(r.read().decode())
            matches=data2.get("matches",[])
            if matches:
                pid=matches[0].get("participantID",{}).get("value","")
                return {"ok":True,"registered":True,"peppol_id":pid or f"0208:{bce}","doc_types":["Voir répertoire PEPPOL"],"source":"Répertoire PEPPOL","dir_url":dir_url}
        except Exception: pass
        return {"ok":False,"error":"Indisponible","registered":False,"peppol_id":"","doc_types":[],"dir_url":dir_url}

def normalize(raw):
    v=raw.strip().upper().replace(".","").replace(" ","").replace("-","")
    if v.startswith("BE"): v=v[2:]
    v="".join(c for c in v if c.isdigit())
    if len(v)==9: v="0"+v
    return v

def check_all(bce):
    if len(bce)!=10:
        e={"ok":False,"error":"Format invalide"}
        return {"bce":bce,"vies":e,"bceData":e,"peppol":e}
    res={}
    def dv():
        with _vies_sem:   res["vies"]  =check_vies(bce)
    def db():
        with _bce_sem:    res["bce"]   =check_bce(bce)
    def dp():
        with _peppol_sem: res["peppol"]=check_peppol(bce)
    ts=[threading.Thread(target=f) for f in (dv,db,dp)]
    for t in ts: t.start()
    for t in ts: t.join()
    return {"bce":bce,"vies":res.get("vies"),"bceData":res.get("bce"),"peppol":res.get("peppol")}

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,fmt,*a): pass
    def do_OPTIONS(self): self.send_response(200); self._cors(); self.end_headers()
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers","Content-Type")
    def do_GET(self):
        if self.path in ("/","/index.html"): self._serve_html()
        elif self.path.startswith("/check?"):
            p=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            bce=normalize(p.get("bce",[""])[0])
            print(f"  /check BE{bce}"); self._json(check_all(bce))
        elif self.path=="/health": self._json({"status":"ok","version":"5"})
        else: self.send_response(404); self.end_headers()
    def do_POST(self):
        if self.path!="/batch": return
        body=json.loads(self.rfile.read(int(self.headers.get("Content-Length",0))).decode())
        numbers=body.get("numbers",[]); total=len(numbers)
        print(f"\n  Batch {total}")
        results=[None]*total; lock=threading.Lock(); done=[0]
        def process(idx,raw):
            bce=normalize(raw); r=check_all(bce)
            with lock:
                results[idx]=r; done[0]+=1; pct=done[0]*100//total
                print(f"\r  [{'='*(pct//5)}{' '*(20-pct//5)}] {done[0]}/{total} ({pct}%) BE{bce}",end="",flush=True)
        pool=[]
        for i,raw in enumerate(numbers):
            while threading.active_count()>VIES_WORKERS*3+20: time.sleep(0.05)
            t=threading.Thread(target=process,args=(i,raw),daemon=True); t.start(); pool.append(t)
            time.sleep(VIES_DELAY/VIES_WORKERS)
        for t in pool: t.join()
        print(f"\n  OK {total}\n"); self._json(results)
    def _serve_html(self):
        p=os.path.join(os.path.dirname(os.path.abspath(__file__)),"app.html")
        if not os.path.exists(p): self.send_response(404); self.end_headers(); self.wfile.write(b"app.html introuvable"); return
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
    is_cloud=os.environ.get("RENDER") or os.environ.get("RAILWAY_ENVIRONMENT")
    print("="*55); print("  VIES + BCE + PEPPOL Checker — Dental Addict v5"); print("="*55)
    print(f"  Port:{PORT}  {'[Cloud]' if is_cloud else '[Local]'}  VIES×{VIES_WORKERS} BCE×{BCE_WORKERS}")
    bind="0.0.0.0" if is_cloud else "localhost"
    with ThreadedServer((bind,PORT),Handler) as srv:
        if not is_cloud:
            import webbrowser
            threading.Thread(target=lambda:(time.sleep(1.2),webbrowser.open(f"http://localhost:{PORT}")),daemon=True).start()
        try: srv.serve_forever()
        except KeyboardInterrupt: print("\n  Arret.")

if __name__=="__main__": main()
