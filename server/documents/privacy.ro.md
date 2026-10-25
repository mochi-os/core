# Confidențialitatea Mochi

Această notificare de confidențialitate explică modul în care **{{operator.name}}** (operatorul acestui server Mochi) colectează, utilizează și partajează datele dumneavoastră personale atunci când vă creați un cont sau utilizați serviciul. {{operator.name}} este operatorul de date.

## Ce colectăm

Atunci când vă înregistrați și utilizați acest server, colectăm:

- **Date de cont**: un nume de utilizator, metodele de autentificare pe care le alegeți (adresă de e-mail, passkeys, aplicație de autentificare, coduri de recuperare sau un cont de la un furnizor terț de autentificare) și orice informații de profil pe care le adăugați (nume afișat, avatar, biografie).
- **Conținutul pe care îl creați**: postări, mesaje, comentarii, fișiere și orice alte date pe care alegeți să le publicați prin aplicațiile instalate pe acest server.
- **Conținut de la alte persoane**: postări, mesaje și detalii de profil ale persoanelor de pe alte servere Mochi și din alte rețele, care ajung pe acest server deoarece dumneavoastră sau alți utilizatori le urmăriți, vă abonați la ele sau corespondați cu ele.
- **Raportări**: o raportare pe care o faceți despre cineva sau pe care cineva o face despre dumneavoastră, pe Mochi sau dintr-o altă rețea, este păstrată pentru ca administratorii acestui server să poată lua măsuri.
- **Date tehnice**: adresa IP, informații despre browser/dispozitiv și marcaje temporale pentru cererile pe care le faceți. Acestea sunt utilizate pentru a menține serviciul sigur și funcțional și pentru a detecta abuzurile.
- **Cookie-uri**: un cookie de sesiune este setat la autentificare. Acesta stochează doar un identificator de sesiune opac; nu folosim cookie-uri pentru publicitate sau pentru urmărire între site-uri.

Nu vindem datele dumneavoastră personale și nu le partajăm cu agenți de publicitate.

## Cum gestionează Mochi datele dumneavoastră (acest lucru este important)

Mochi este o **rețea peer-to-peer (P2P)**, nu o platformă cu un singur server. Aceasta schimbă modul în care circulă datele dumneavoastră, în moduri care contează pentru confidențialitatea dumneavoastră:

- Conținutul pe care îl publicați urmăritorilor, abonaților sau spațiilor partajate este **replicat pe alte servere Mochi** care găzduiesc acei urmăritori sau abonați. {{operator.name}} nu poate șterge copii ale conținutului dumneavoastră de pe servere pe care nu le operează. Dacă ștergeți o postare, acest server va trimite o notificare de ștergere către peer-ii cărora le-a trimis anterior postarea, însă ștergerea se face în limita posibilităților și depinde de cooperarea acelor peer-i.
- **Identitatea dumneavoastră (nume de utilizator, profil, chei publice)** este publicată în directorul Mochi dacă alegeți un profil public, astfel încât alți utilizatori Mochi să vă poată găsi și contacta.
- Conținutul direct și privat (ciorne, note private, mesaje private între destinatari specifici) rămâne doar pe acest server și pe serverele destinatarilor; nu face parte din rețeaua P2P publică.
- Acolo unde acest server vă permite să vă **conectați la alte rețele** - prin asocierea unui cont pe care îl dețineți la un alt serviciu sau prin găzduirea aici a unui cont pe care persoanele din alte rețele îl pot urmări sau căruia îi pot trimite mesaje - ceea ce publicați sau trimiteți prin acel cont ajunge la servere din acele rețele. Acestea nu sunt servere Mochi și {{operator.name}} nu le operează. Un astfel de cont este public în rețeaua sa și, în unele rețele, la fel sunt și conturile pe care le urmăriți, postările pe care le marcați cu „Îmi place” și, pe Bluesky, persoanele pe care le blocați. Dacă editați sau ștergeți ulterior ceva, se trimite o cerere pe care acele servere o pot ignora.
- Persoanele cu care conversați pe chat, pe Mochi și în alte rețele, pot vedea când sunteți online. Puteți dezactiva acest lucru cu **Arată când sunt conectat** în setările aplicației de chat.
- O raportare pe care o faceți despre cineva de pe un alt server poate fi trimisă și moderatorilor acelui server, ca raportare a acestui server, nu a dumneavoastră: aceasta nu precizează cine a făcut-o.

Pe scurt: orice alegeți să partajați cu alte persoane, pe Mochi sau în alte rețele, va circula dincolo de controlul {{operator.name}}, prin design. Tratați conținutul pe care îl publicați așa cum ați trata e-mailul sau orice altă comunicare federată.

## Terți

Partajăm minimul de date personale necesare cu următorii terți atunci când utilizați funcțiile aferente:

- **Serviciu de e-mail**: e-mailurile trimise (coduri de autentificare, verificarea adresei, notificări) sunt expediate printr-un furnizor SMTP configurat de {{operator.name}}.
- **Furnizori terți de autentificare**: dacă vă autentificați cu Google, GitHub, Microsoft, Facebook sau alt furnizor OAuth, acel furnizor află că v-ați autentificat pe acest server și ne partajează adresa de e-mail și datele de bază ale profilului.
- **Furnizori de plăți**: dacă efectuați o achiziție printr-o aplicație de marketplace, plata este procesată de Stripe (sau de furnizorul de plăți pe care l-a configurat {{operator.name}}). Nu stocăm detalii complete ale cardului.
- **Servicii de notificări push**: notificările către aplicația Mochi pe Android sunt livrate prin Firebase Cloud Messaging de la Google, care transportă titlul și textul acestora. Dacă vă autentificați pe acest server cu o altă aplicație, cum ar fi o aplicație Mastodon, Matrix sau XMPP, notificările acesteia sunt livrate prin serviciul push pe care îl folosește aplicația respectivă și pot include mesajul la care se referă notificarea.
- **Autoritate de certificare**: acolo unde acest server găzduiește conturi Bluesky sub propriul domeniu, pentru adresa fiecărui cont se emite un certificat de securitate. Autoritățile de certificare publică fiecare certificat pe care îl emit în jurnale publice, astfel încât acele adrese sunt listate acolo.
- **Alte rețele**: dacă asociați un cont pe care îl dețineți la un alt serviciu, acel serviciu primește ceea ce postați prin intermediul contului respectiv, iar acest server stochează credențialele care îi permit să acționeze acolo în numele dumneavoastră. Dacă găzduiți aici un cont pentru o altă rețea, serverele și serviciile acelei rețele primesc ceea ce publicați în ea, împreună cu adresa publică a contului.

## Drepturile dumneavoastră

În funcție de locul în care locuiți, este posibil să aveți dreptul de a accesa, corecta, exporta, restricționa sau șterge datele dumneavoastră personale. Vă puteți exporta datele și vă puteți închide contul chiar dumneavoastră, din Setări. Închiderea contului îl blochează pentru o perioadă de grație, în care vă puteți autentifica și puteți anula închiderea; după aceea, contul și datele sale sunt eliminate de pe acest server (sub rezerva păstrării copiilor de rezervă și a oricărei obligații legale de a păstra anumite înregistrări). Pentru a exercita orice alt drept sau dacă nu vă puteți autentifica, trimiteți un e-mail la **{{operator.email}}**.

## Păstrarea datelor

Păstrăm datele contului dumneavoastră atât timp cât contul este activ și pentru o perioadă rezonabilă după închidere, pentru a recupera date în caz de ștergere accidentală și pentru a îndeplini obligațiile legale și de audit. Copiile de rezervă sunt păstrate de obicei până la 90 de zile.

## Unde ne aflăm

Acest serviciu este operat din **{{operator.jurisdiction}}**, iar legea din {{operator.jurisdiction}} se aplică prelucrării datelor dumneavoastră personale pe acest server.

## Modificări

Putem actualiza acești termeni din când în când. Modificările semnificative vor fi anunțate înainte de a intra în vigoare. Utilizarea în continuare a serviciului după o modificare înseamnă că acceptați termenii actualizați.

## Contact

Pentru orice întrebare privind confidențialitatea sau pentru a face o solicitare: **{{operator.email}}**
