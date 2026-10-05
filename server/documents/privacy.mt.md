# Privatezza ta' Mochi

Din l-avviż dwar il-privatezza jispjega kif **{{operator.name}}** (l-operatur ta' dan is-server Mochi) jiġbor, juża, u jaqsam id-dejta personali tiegħek meta toħloq kont jew tuża s-servizz. {{operator.name}} huwa l-kontrollur tad-dejta.

## X'niġbru

Meta tirreġistra u tuża dan is-server niġbru:

- **Dejta tal-kont**: isem tal-utent, il-metodi ta' awtentikazzjoni li tagħżel (indirizz email, passkeys, app awtentikatur, kodiċijiet ta' rkupru, jew kont mingħand fornitur ta' login terz), u kwalunkwe informazzjoni tal-profil li żżid (isem li jidher, avatar, bijografija).
- **Kontenut li toħloq**: posts, messaġġi, kummenti, fajls, u kwalunkwe dejta oħra li tagħżel li tippubblika permezz tal-apps installati fuq dan is-server.
- **Kontenut minn nies oħra**: posts, messaġġi, u dettalji tal-profil ta' nies fuq servers Mochi oħra u fuq netwerks oħra, li jaslu f'dan is-server għax int jew utenti oħra jsegwuhom, jabbonaw magħhom, jew jikkorrispondu magħhom.
- **Dejta teknika**: indirizz IP, informazzjoni dwar il-browser/apparat, u timestamps għat-talbiet li tagħmel. Din tintuża biex is-servizz jibqa' sigur u jaħdem, u biex jiġi identifikat l-abbuż.
- **Cookies**: tiġi stabbilita session cookie meta tidħol. Hija taħżen biss identifikatur opak tas-sessjoni; ma nużawx cookies għar-reklamar jew għat-traċċar bejn siti.

Aħna ma nbigħux id-dejta personali tiegħek, u ma naqsmuhiex ma' min jirreklama.

## Kif Mochi jittratta d-dejta tiegħek (dan huwa importanti)

Mochi huwa **netwerk peer-to-peer (P2P)**, mhux pjattaforma ta' server wieħed. Dan jibdel kif id-dejta tiegħek tgħaddi b'modi li huma importanti għall-privatezza tiegħek:

- Il-kontenut li tippubblika lis-segwaċi, abbonati, jew spazji kondiviżi jiġi **replikat fuq servers Mochi oħra** li jospitaw dawk is-segwaċi jew abbonati. {{operator.name}} ma jistax iħassar il-kopji tal-kontenut tiegħek minn servers li ma jħaddimx hu. Jekk tħassar post, dan is-server jibgħat avviż ta' tħassir lill-peers li bagħat il-post lilhom qabel, iżda t-tħassir huwa best-effort u jiddependi mill-kooperazzjoni ta' dawk il-peers.
- L-**identità tiegħek (isem tal-utent, profil, ċwievet pubbliċi)** tiġi ppubblikata fid-direttorju ta' Mochi jekk tagħżel profil pubbliku, biex utenti oħra ta' Mochi jistgħu jsibuk u jikkuntattjawk.
- Il-kontenut dirett u privat (drafts, noti privati, messaġġi privati bejn riċevituri speċifiċi) jibqa' fuq dan is-server u fuq is-servers tar-riċevituri biss; mhuwiex parti mill-network P2P pubbliku.
- Fejn dan is-server jippermettilek **tikkonnettja ma' netwerks oħra** - billi torbot kont li għandek fuq servizz ieħor, jew billi tospita kont hawn li nies fuq netwerks oħra jistgħu jsegwu jew jibagħtulu messaġġi - dak li tippubblika jew tibgħat permezz ta' dak il-kont imur fuq servers ta' dawk in-netwerks. Dawn mhumiex servers Mochi u {{operator.name}} ma jħaddimhomx. Kont bħal dan huwa pubbliku fuq in-netwerk tiegħu, u fuq xi netwerks hekk ukoll huma l-kontijiet li ssegwi u l-posts li jogħġbuk. L-editjar jew it-tħassir ta' xi ħaġa aktar tard jibgħat talba li dawk is-servers jistgħu jinjoraw.

Fil-qosor: kwalunkwe ħaġa li tagħżel li taqsam ma' nies oħra, fuq Mochi jew fuq netwerks oħra, se ssiefer lil hinn mill-kontroll ta' {{operator.name}}, b'mod intenzjonat. Ittratta l-kontenut li tippubblika bħallikieku kien email jew kwalunkwe komunikazzjoni federata oħra.

## Partijiet terzi

Aħna naqsmu d-dejta personali minima meħtieġa mal-partijiet terzi li ġejjin meta tuża l-funzjonijiet relatati:

- **Servizz tal-email**: l-emails li joħorġu (kodiċijiet tad-dħul, verifika tal-indirizz, notifiki) jintbagħtu permezz ta' fornitur SMTP konfigurat minn {{operator.name}}.
- **Fornituri ta' login terzi**: jekk tidħol b'Google, GitHub, Microsoft, Facebook, jew fornitur OAuth ieħor, dak il-fornitur isir jaf li dħalt f'dan is-server u jaqsam magħna l-indirizz email u dejta bażika tal-profil tiegħek.
- **Fornituri ta' ħlas**: jekk tagħmel xirja permezz ta' app ta' marketplace, il-ħlas jiġi pproċessat minn Stripe (jew kwalunkwe fornitur ta' ħlas li {{operator.name}} ikun ikkonfigura). Aħna ma naħżnux id-dettalji sħaħ tal-karta.
- **Netwerks oħra**: jekk torbot kont li għandek fuq servizz ieħor, dak is-servizz jirċievi dak li tippubblika permezz tiegħu, u dan is-server jaħżen il-kredenzjali li tippermettilu jaġixxi f'ismek hemmhekk. Jekk tospita kont hawn għal netwerk ieħor, is-servers u s-servizzi ta' dak in-netwerk jirċievu dak li tippubblika fih, flimkien mal-indirizz pubbliku tal-kont.

## Id-drittijiet tiegħek

Skont fejn tgħix, jista' jkollok id-dritt li taċċessa, tikkoreġi, tesporta, tirrestrinġi, jew tħassar id-dejta personali tiegħek. Tista' tesporta d-dejta tiegħek u tagħlaq il-kont tiegħek int stess fl-Issettjar. Meta tagħlaq il-kont tiegħek, dan jissakkar għal perjodu ta' grazzja, li matulu tista' tidħol u tikkanċella l-għeluq; wara dan il-kont u d-dejta tiegħu jitneħħew minn dan is-server (suġġett għaż-żamma tal-backups u kwalunkwe obbligu legali li nżommu rekords speċifiċi). Biex teżerċita kwalunkwe dritt ieħor, jew jekk ma tistax tidħol, ibgħat email lil **{{operator.email}}**.

## Żamma tad-dejta

Inżommu d-dejta tal-kont tiegħek sakemm il-kont tiegħek ikun attiv, u għal perjodu raġonevoli wara l-għeluq biex nirkupraw minn tħassir aċċidentali u biex nissodisfaw l-obbligi legali u tal-awditjar. Il-backups normalment jinżammu sa 90 ġurnata.

## Fejn ninsabu

Dan is-servizz huwa operat minn **{{operator.jurisdiction}}**, u l-liġi ta' {{operator.jurisdiction}} tapplika għall-ipproċessar tad-dejta personali tiegħek fuq dan is-server.

## Tibdil

Aħna jista' jkollna naġġornaw dawn it-termini minn żmien għal żmien. It-tibdil materjali jitħabbar qabel ma jidħol fis-seħħ. L-użu kontinwu tas-servizz wara tibdil ifisser li taċċetta t-termini aġġornati.

## Kuntatt

Għal kull mistoqsija dwar il-privatezza jew biex tagħmel talba: **{{operator.email}}**
