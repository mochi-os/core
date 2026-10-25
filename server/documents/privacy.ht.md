# Konfidansyalite Mochi

Avi konfidansyalite sa a esplike kijan **{{operator.name}}** (operatè sèvè Mochi sa a) kolekte, itilize ak pataje done pèsonèl ou yo lè ou kreye yon kont oswa ou itilize sèvis la. {{operator.name}} se kontwolè done yo.

## Sa nou kolekte

Lè ou enskri epi w ap itilize sèvè sa a, nou kolekte :

- **Done kont** : yon non itilizatè, metòd otantifikasyon ou chwazi yo (adrès imèl, passkey, aplikasyon otantifikasyon, kòd rekiperasyon, oswa yon kont ki soti nan yon founisè koneksyon tyès), ak nenpòt enfòmasyon pwofil ou ajoute (non afiche, avata, byografi).
- **Kontni ou kreye** : piblikasyon, mesaj, kòmantè, fichye, ak nenpòt lòt done ou chwazi pou pibliye atravè aplikasyon ki enstale sou sèvè sa a.
- **Kontni lòt moun** : piblikasyon, mesaj, ak detay pwofil moun ki sou lòt sèvè Mochi ak sou lòt rezo, ki rive sou sèvè sa a paske ou menm oswa lòt itilizatè swiv yo, abòne ak yo, oswa koresponn ak yo.
- **Rapò** : yon rapò ou fè sou yon moun, oswa yon moun fè sou ou, sou Mochi oswa ki soti nan yon lòt rezo, kenbe pou administratè sèvè sa a ka aji sou li.
- **Done teknik** : adrès IP, enfòmasyon sou navigatè/aparèy, ak orodataj demand ou yo. Sa a sèvi pou kenbe sèvis la an sekirite ak fonksyonèl, epi pou detekte abi.
- **Cookies** : yon cookie sesyon plase lè ou konekte. Li sèvi sèlman pou estoke yon idantifyan sesyon opak ; nou pa itilize cookies pou piblisite oswa pou swiv ant sit.

Nou pa vann done pèsonèl ou yo, epi nou pa pataje yo ak piblisitè.

## Kijan Mochi jere done ou yo (sa a enpòtan)

Mochi se yon **rezo peer-to-peer (P2P)**, se pa yon platfòm ki gen yon sèl sèvè. Sa chanje fason done ou yo sikile nan yon mannyè ki konsène vi prive ou :

- Kontni ou pibliye pou abonè, swivè, oswa espas pataje yo **replike sou lòt sèvè Mochi** ki abrite swivè oswa abonè sa yo. {{operator.name}} pa kapab efase kopi kontni ou yo sou sèvè li pa egzekite. Si ou efase yon piblikasyon, sèvè sa a ap voye yon avi sou efasman an bay tout pè li te voye piblikasyon an deja, men efasman an se yon efò omeyè epi li depann sou koperasyon pè sa yo.
- **Idantite ou (non itilizatè, pwofil, kle piblik)** pibliye nan anyè Mochi a si ou chwazi yon pwofil piblik, pou lòt itilizatè Mochi yo ka jwenn ou epi kontakte ou.
- Kontni dirèk ki prive (bouyon, nòt prive, mesaj prive ant destinatè espesifik) rete sèlman sou sèvè sa a ak sèvè destinatè yo ; li pa fè pati rezo P2P piblik la.
- Kote sèvè sa a pèmèt ou **konekte ak lòt rezo** - swa lè ou lye yon kont ou genyen sou yon lòt sèvis, swa lè ou ebèje isit la yon kont moun ki sou lòt rezo ka swiv oswa voye mesaj bay - sa ou pibliye oswa voye atravè kont sa a ale sou sèvè ki sou rezo sa yo. Se pa sèvè Mochi yo ye, epi {{operator.name}} pa egzekite yo. Yon kont konsa piblik sou rezo li a, epi sou kèk rezo, kont ou swiv yo, piblikasyon ou renmen yo epi, sou Bluesky, moun ou bloke yo piblik tou. Si ou modifye oswa efase yon bagay pita, sa voye yon demand sèvè sa yo ka inyore.
- Moun ou diskite avè yo, sou Mochi ak sou lòt rezo, ka wè lè ou anliy. Ou ka dezaktive sa ak **Montre lè mwen anliy** nan paramèt aplikasyon diskisyon an.
- Yon rapò ou fè sou yon moun ki sou yon lòt sèvè ka voye tou bay moderatè lòt sèvè a, kòm rapò sèvè sa a epi pa kòm rapò pa w : li pa di kiyès ki te fè l.

An rezime : tout sa ou chwazi pataje ak lòt moun, sou Mochi oswa sou lòt rezo, pral vwayaje pi lwen pase kontwòl {{operator.name}}, jan li fèt. Trete kontni ou pibliye menm jan ou ta trete imèl oswa nenpòt lòt kominikasyon federe.

## Tyès pati

Nou pataje minimòm done pèsonèl ki nesesè ak tyès pati sa yo lè ou itilize karakteristik ki gen rapò ak yo :

- **Sèvis imèl** : imèl ki soti (kòd koneksyon, verifikasyon adrès, notifikasyon) voye atravè yon founisè SMTP {{operator.name}} konfigire.
- **Founisè koneksyon tyès** : si ou konekte ak Google, GitHub, Microsoft, Facebook, oswa yon lòt founisè OAuth, founisè sa a ap aprann ou te konekte sou sèvè sa a epi li pataje adrès imèl ou ak done debaz pwofil ou ak nou.
- **Founisè peman** : si ou fè yon acha atravè yon aplikasyon mache, peman an trete pa Stripe (oswa nenpòt founisè peman {{operator.name}} konfigire). Nou pa estoke detay konplè kat yo.
- **Sèvis notifikasyon push** : notifikasyon pou aplikasyon Mochi sou Android yo livre atravè Firebase Cloud Messaging, sèvis Google la, ki pote tit ak tèks yo. Si ou konekte sou sèvè sa a ak yon lòt aplikasyon, tankou yon aplikasyon Mastodon, Matrix oswa XMPP, notifikasyon li yo livre atravè sèvis push aplikasyon sa a itilize a, epi yo ka gen ladan yo mesaj notifikasyon an konsène a.
- **Otorite sètifikasyon** : kote sèvè sa a ebèje kont Bluesky anba pwòp domèn li, yo bay adrès chak kont yon sètifika sekirite. Otorite sètifikasyon yo pibliye chak sètifika yo bay nan jounal piblik, kidonk adrès sa yo parèt la.
- **Lòt rezo** : si ou lye yon kont ou genyen sou yon lòt sèvis, sèvis sa a resevwa sa ou pibliye atravè li, epi sèvè sa a estoke kalifikasyon ki pèmèt li aji pou ou la. Si ou ebèje isit la yon kont pou yon lòt rezo, sèvè ak sèvis rezo sa a resevwa sa ou pibliye sou li, ansanm ak adrès piblik kont la.

## Dwa ou yo

Selon kote ou rete, ou ka gen dwa pou jwenn aksè, korije, ekspòte, restrikte, oswa efase done pèsonèl ou yo. Ou ka ekspòte done ou yo epi fèmen kont ou poukont ou nan Paramèt. Fèmti kont ou bloke li pou yon peryòd gras, pandan ki ou ka konekte epi anile ; apre sa, kont la ak done li yo retire sou sèvè sa a (sijè a konsèvasyon backup ak nenpòt obligasyon legal pou kenbe dosye espesifik). Pou egzèse nenpòt lòt dwa, oswa si ou pa ka konekte, voye imèl bay **{{operator.email}}**.

## Konsèvasyon done

Nou kenbe done kont ou yo otank kont ou aktif, epi pou yon peryòd rezonab apre fèmti li pou n ka rekipere apre yon efasman aksidantèl epi pou n satisfè obligasyon legal ak odit. Backup yo jeneralman konsève pou jiska 90 jou.

## Kote nou ye

Sèvis sa a opere depi **{{operator.jurisdiction}}**, epi lwa {{operator.jurisdiction}} aplike pou tretman done pèsonèl ou yo sou sèvè sa a.

## Chanjman

Nou ka mete kondisyon sa yo ajou de tan zan tan. Chanjman enpòtan yo pral anonse anvan yo antre an vigè. Si w kontinye itilize sèvis la apre yon chanjman, sa vle di ou aksepte kondisyon ki ajou yo.

## Kontak

Pou nenpòt kesyon sou konfidansyalite oswa pou fè yon demand : **{{operator.email}}**
