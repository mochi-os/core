# Privatësia e Mochi

Ky njoftim privatësie shpjegon se si **{{operator.name}}** (operatori i këtij serveri Mochi) mbledh, përdor dhe ndan të dhënat tuaja personale kur krijoni një llogari ose përdorni shërbimin. {{operator.name}} është kontrolluesi i të dhënave.

## Çfarë mbledhim

Kur regjistroheni dhe përdorni këtë server, ne mbledhim:

- **Të dhënat e llogarisë**: një emër përdoruesi, metodat e vërtetimit që zgjidhni (adresë email, çelësa kalimi, aplikacion vërtetuesi, kode rikuperimi, ose një llogari nga një ofrues hyrjeje i palës së tretë), dhe çdo informacion profili që shtoni (emri i shfaqur, avatari, biografia).
- **Përmbajtjen që krijoni**: postimet, mesazhet, komentet, skedarët dhe çdo të dhënë tjetër që zgjidhni të publikoni përmes aplikacioneve të instaluara në këtë server.
- **Përmbajtjen nga njerëz të tjerë**: postimet, mesazhet dhe detajet e profilit të njerëzve në serverë të tjerë Mochi dhe në rrjete të tjera, të cilat arrijnë në këtë server sepse ju ose përdorues të tjerë i ndiqni, abonoheni tek ata ose korrespondoni me ta.
- **Të dhënat teknike**: adresa IP, informacioni i shfletuesit/pajisjes dhe vulat kohore për kërkesat që bëni. Këto përdoren për ta mbajtur shërbimin të sigurt dhe funksional, dhe për të zbuluar abuzimet.
- **Cookies**: një cookie sesioni vendoset kur hyni. Ajo ruan vetëm një identifikues sesioni të errët; ne nuk i përdorim cookies për reklama ose për gjurmim ndër-faqesh.

Ne nuk i shesim të dhënat tuaja personale dhe nuk i ndajmë ato me reklamuesit.

## Si i trajton Mochi të dhënat tuaja (kjo është e rëndësishme)

Mochi është një **rrjet peer-to-peer (P2P)**, jo një platformë me një server të vetëm. Kjo ndryshon mënyrën se si rrjedhin të dhënat tuaja, në mënyra që kanë rëndësi për privatësinë tuaj:

- Përmbajtja që publikoni për ndjekësit, abonuesit ose hapësirat e përbashkëta **përsëritet në serverë të tjerë Mochi** që presin këta ndjekës ose abonues. {{operator.name}} nuk mund të fshijë kopjet e përmbajtjes suaj nga serverë që nuk i operon. Nëse fshini një postim, ky server do t'u dërgojë një njoftim fshirjeje peer-ave që i ka dërguar postimin më parë, por fshirja është "best-effort" dhe varet nga bashkëpunimi i atyre peer-ave.
- **Identiteti juaj (emri i përdoruesit, profili, çelësat publikë)** publikohet në direktorinë e Mochi nëse zgjidhni një profil publik, kështu që përdoruesit e tjerë të Mochi mund t'ju gjejnë dhe kontaktojnë.
- Përmbajtja e drejtpërdrejtë dhe private (draftet, shënimet private, mesazhet private mes marrësve specifikë) qëndron vetëm në këtë server dhe në serverët e marrësve; ajo nuk është pjesë e rrjetit publik P2P.
- Aty ku ky server ju lejon të **lidheni me rrjete të tjera** - duke lidhur një llogari që keni në një shërbim tjetër, ose duke pritur këtu një llogari që njerëzit në rrjete të tjera mund ta ndjekin ose t'i dërgojnë mesazhe - ajo që publikoni ose dërgoni përmes asaj llogarie shkon te serverët në ato rrjete. Ata nuk janë serverë Mochi dhe {{operator.name}} nuk i operon. Një llogari e tillë është publike në rrjetin e saj, dhe në disa rrjete publike janë gjithashtu llogaritë që ndiqni dhe postimet të cilave u jepni pëlqim. Redaktimi ose fshirja e diçkaje më vonë dërgon një kërkesë që ata serverë mund ta shpërfillin.

Shkurt: çdo gjë që zgjidhni të ndani me njerëz të tjerë, në Mochi ose në rrjete të tjera, do të udhëtojë përtej kontrollit të {{operator.name}}, sipas dizajnit. Trajtoni përmbajtjen që publikoni ashtu si do të trajtonit emailin ose çdo komunikim tjetër të federuar.

## Palët e treta

Ne ndajmë minimumin e të dhënave personale të kërkuara me palët e treta të mëposhtme kur përdorni veçoritë përkatëse:

- **Shërbimi i emailit**: emailet dalëse (kodet e hyrjes, verifikimi i adresës, njoftimet) dërgohen përmes një ofruesi SMTP të konfiguruar nga {{operator.name}}.
- **Ofruesit e hyrjes të palës së tretë**: nëse hyni me Google, GitHub, Microsoft, Facebook ose një ofrues tjetër OAuth, ai ofrues mëson që ju keni hyrë në këtë server dhe ndan adresën tuaj të emailit dhe të dhënat bazë të profilit me ne.
- **Ofruesit e pagesave**: nëse bëni një blerje përmes një aplikacioni tregu, pagesa përpunohet nga Stripe (ose cilido ofrues pagese që ka konfiguruar {{operator.name}}). Ne nuk ruajmë detajet e plota të kartës.
- **Rrjetet e tjera**: nëse lidhni një llogari që keni në një shërbim tjetër, ai shërbim merr atë që postoni përmes asaj llogarie, dhe ky server ruan kredencialin që i lejon atij të veprojë atje në emrin tuaj. Nëse prisni këtu një llogari për një rrjet tjetër, serverët dhe shërbimet e atij rrjeti marrin atë që publikoni në të, së bashku me adresën publike të llogarisë.

## Të drejtat tuaja

Në varësi të vendit ku jetoni, mund të keni të drejta për të hyrë, korrigjuar, eksportuar, kufizuar ose fshirë të dhënat tuaja personale. Ju mund t'i eksportoni të dhënat tuaja dhe ta mbyllni llogarinë vetë te Cilësimet. Mbyllja e llogarisë e bllokon atë për një periudhë hiri, gjatë së cilës mund të hyni dhe të anuloni mbylljen; pas kësaj, llogaria dhe të dhënat e saj hiqen nga ky server (në varësi të mbajtjes së kopjeve rezervë dhe çdo detyrimi ligjor për të ruajtur të dhëna specifike). Për të ushtruar çdo të drejtë tjetër, ose nëse nuk mund të hyni, dërgoni email në **{{operator.email}}**.

## Mbajtja e të dhënave

Ne i mbajmë të dhënat e llogarisë suaj për aq kohë sa llogaria juaj është aktive, dhe për një periudhë të arsyeshme pas mbylljes për t'u rikuperuar nga fshirjet aksidentale dhe për të përmbushur detyrimet ligjore dhe të auditimit. Kopjet rezervë zakonisht mbahen deri në 90 ditë.

## Ku ndodhemi

Ky shërbim operohet nga **{{operator.jurisdiction}}**, dhe ligji i {{operator.jurisdiction}} zbatohet për përpunimin e të dhënave tuaja personale në këtë server.

## Ndryshimet

Ne mund t'i përditësojmë këto kushte herë pas here. Ndryshimet thelbësore do të njoftohen para se të hyjnë në fuqi. Përdorimi i vazhdueshëm i shërbimit pas një ndryshimi nënkupton që ju i pranoni kushtet e përditësuara.

## Kontakti

Për çdo pyetje rreth privatësisë ose për të bërë një kërkesë: **{{operator.email}}**
