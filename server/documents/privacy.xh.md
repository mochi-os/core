# Ubumfihlo beMochi

Esi saziso sobumfihlo sichaza indlela u **{{operator.name}}** (umqhubi wale seva yeMochi) ahlanganisa, asebenzise, kwaye abelane ngedata yakho yobuqu xa udala iakhawunti okanye usebenzisa inkonzo. {{operator.name}} ungumlawuli wedata.

## Yintoni esiyihlanganisayo

Xa ubhalisa kwaye usebenzisa le seva sihlanganisa:

- **Idata yeakhawunti**: igama lomsebenzisi, iindlela zoqinisekiso ozikhethayo (idilesi ye-imeyile, ii-passkey, iapp yoqinisekiso, iikhowudi zokufumana kwakhona, okanye iakhawunti evela kumboneleli wokungena weqela lesithathu), kunye nalo naluphi na ulwazi lweprofayile olongezayo (igama lokubonisa, isithombe, ibhayografi).
- **Umxholo owudalayo**: iiposti, imiyalezo, izimvo, iifayile, kunye nayo nayiphi na enye idata oyikhethayo ukuyipapasha kwiiapp ezifakwe kule seva.
- **Umxholo ovela kwabanye abantu**: iiposti, imiyalezo, kunye neenkcukacha zeprofayile zabantu abakwezinye iiseva zeMochi nakwezinye iinethiwekhi, ezifika kule seva ngenxa yokuba wena okanye abanye abasebenzisi niyabalandela, nibhalisile kubo, okanye nibhalelana nabo.
- **Iingxelo**: ingxelo oyenzayo ngomntu othile, okanye eyenziwa ngomntu othile ngawe, kuMochi okanye isuka kwenye inethiwekhi, iyagcinwa ukuze abalawuli bale seva bakwazi ukuthatha amanyathelo ngayo.
- **Idata yobuchwepheshe**: idilesi ye-IP, ulwazi lwesibrowza/lwesixhobo, kunye nezitembu zexesha zezicelo ozenzayo. Oku kusetyenziselwa ukugcina inkonzo ikhuselekile kwaye isebenza, kwaye ukubona ukuxhatshazwa.
- **Iikuki**: ikuki yeseshini imiselwa xa ungena. Igcina kuphela isichongi seseshini esingaqondakaliyo; asisebenzisi iikuki kwintengiso okanye ukulandelelwa phakathi kweesayithi.

Asithengisi idata yakho yobuqu, kwaye asabelani nayo nabantengisi.

## Indlela uMochi aphatha ngayo idata yakho (oku kubalulekile)

UMochi yi **nethiwekhi yontanga-kuntanga (P2P)**, hayi iqonga leseva enye. Oku kuyatshintsha indlela idata yakho ehamba ngayo ngeendlela ezibalulekileyo kubumfihlo bakho:

- Umxholo owupapashayo kubalandeli, ababhalisi, okanye iindawo ezabelwanayo **ukhutshelwa kwezinye iiseva zeMochi** eziphethe abo balandeli okanye ababhalisi. {{operator.name}} akakwazi ukucima iikopi zomxholo wakho kwiiseva angaziqhubiyo. Ukuba ucima iposti, le seva iya kuthumela isaziso sokucima koontanga ebebethumelelwe iposti ngaphambili, kodwa ukucima yimizamo engcono kwaye kuxhomekeke kwintsebenziswano yaloo bantanga.
- **Ubuwena bakho (igama lomsebenzisi, iprofayile, izitshixo zoluntu)** zipapashwa kuluhlu lwemibhalo lukaMochi ukuba ukhetha iprofayile yoluntu, ukuze abanye abasebenzisi beMochi bakwazi ukukufumana baqhagamshelane nawe.
- Umxholo wabucala ongqalileyo (iidrafti, amanqaku abucala, imiyalezo yabucala phakathi kwabamkeli abathile) uhlala kule seva nakwiiseva zabamkeli kuphela; awuyonxalenye yenethiwekhi ye-P2P yoluntu.
- Apho le seva ikuvumela ukuba **uqhagamshele kwezinye iinethiwekhi** - ngokudibanisa iakhawunti onayo kwenye inkonzo, okanye ngokuphatha apha iakhawunti abantu abakwezinye iinethiwekhi abanokuyilandela okanye bayithumelele imiyalezo - into oyipapashayo okanye oyithumelayo ngaloo akhawunti iya kwiiseva ezikwezo nethiwekhi. Azizizo iiseva zeMochi kwaye {{operator.name}} akaziqhubi. Iakhawunti enjalo ibonakala esidlangalaleni kwinethiwekhi yayo, kwaye kwezinye iinethiwekhi iiakhawunti ozilandelayo neeposti ozithandileyo nazo zibonakala esidlangalaleni, kwaye kuBluesky nabantu obavimbileyo bayabonakala esidlangalaleni. Ukuhlela okanye ukucima into kamva kuthumela isicelo esinokungahoywa zezo seva.
- Abantu oncokola nabo, kuMochi nakwezinye iinethiwekhi, bayakwazi ukubona xa uqhagamshelwe. Ungakucima oku usebenzisa **Bonisa xa ndiqhagamshelwe** kwiisetingi zeapp yencoko.
- Ingxelo oyenzayo ngomntu okwenye iseva ingathunyelwa nakubalawuli bomxholo baloo seva, njengengxelo yale seva hayi eyakho: ayichazi ukuba ngubani oyenzileyo.

Ngamafutshane: nantoni na oyikhethayo ukwabelana ngayo nabanye abantu, kuMochi okanye kwezinye iinethiwekhi, iya kuhamba ngaphaya kolawulo lwe {{operator.name}}, ngokuyilwa. Phatha umxholo opapashayo njengoko ungaphatha i-imeyile okanye nayiphi na enye intetho enxulumene.

## Amaqela esithathu

Sabelana ngedata encinci kakhulu yobuqu efunekayo namaqela esithathu alandelayo xa usebenzisa iimpawu ezinxulumeneyo:

- **Inkonzo ye-imeyile**: ii-imeyile eziphumayo (iikhowudi zokungena, ukuqinisekiswa kwedilesi, izaziso) zithunyelwa ngomboneleli we-SMTP omiselweyo ngu {{operator.name}}.
- **Ababoneleli bokungena beqela lesithathu**: ukuba ungena ngeGoogle, GitHub, Microsoft, Facebook, okanye omnye umboneleli we-OAuth, loo mboneleli ufunda ukuba ungene kule seva yaye wabelana ngedilesi yakho ye-imeyile nedata esisiseko yeprofayile nathi.
- **Ababoneleli beentlawulo**: ukuba wenza intengo ngeapp yemarike, intlawulo iqhutywa nguStripe (okanye nawuphi na umboneleli weentlawulo {{operator.name}} amisileyo). Asigcini iinkcukacha zekhadi epheleleyo.
- **Iinkonzo zezaziso ze-push**: izaziso eziya kwiapp yeMochi kwi-Android zithunyelwa ngeFirebase Cloud Messaging kaGoogle, ethwala isihloko nombhalo wazo. Ukuba ungena kule seva ngenye iapp, efana neapp yeMastodon, Matrix okanye XMPP, izaziso zayo zithunyelwa ngenkonzo ye-push esetyenziswa yiloo app, kwaye zingaquka umyalezo ochaphazelwa sisaziso.
- **Igunya lezatifikethi**: apho le seva iphethe iiakhawunti zeBluesky phantsi kwedomeyini yayo, idilesi yeakhawunti nganye ikhutshelwa isatifikethi sokhuseleko. Amagunya ezatifikethi apapasha sonke isatifikethi awasikhuphayo kwiirekhodi zoluntu, ngoko ezo dilesi zidweliswe apho.
- **Ezinye iinethiwekhi**: ukuba udibanisa iakhawunti onayo kwenye inkonzo, loo nkonzo ifumana into oyiposayo ngayo, kwaye le seva igcina isiqinisekiso esiyivumela ukuba isebenze egameni lakho apho. Ukuba uphatha apha iakhawunti yenye inethiwekhi, iiseva neenkonzo zaloo nethiwekhi zifumana into oyipapashayo kuyo, kunye nedilesi yoluntu yeakhawunti.

## Amalungelo akho

Ngokuxhomekeke kwapho uhlala khona, unokuba namalungelo okufikelela, ukulungisa, ukuthumela, ukuthintela, okanye ukucima idata yakho yobuqu. Ungayithumela ngaphandle idata yakho kwaye uvale iakhawunti yakho ngokwakho kwi-Iisetingi. Ukuvala iakhawunti yakho kuyitshixa ixesha lesisa, apho unokungena khona urhoxise ukuvalwa kwayo; emva koko iakhawunti nedata yayo zisuswa kule seva (ngokuxhomekeke ekugcineni kwekhophi yokhuselo nakuso nasiphi na umsebenzi womthetho wokugcina iimbali ezithile). Ukusebenzisa naliphi na elinye ilungelo, okanye ukuba awukwazi ukungena, thumela i-imeyile ku **{{operator.email}}**.

## Ukugcinwa kwedata

Sigcina idata yakho yeakhawunti ixesha elide njengokuba iakhawunti yakho isebenza, nakwithuba elifanelekileyo emva kokuvalwa ukubuyisa ekucinyweni okungenangqondo nokwanelisa imisebenzi yomthetho noyolowuko. Iikhophi zokhuselo zihlala zigcinwa iintsuku ezifika kwi-90.

## Sikhona phi

Le nkonzo iqhutywa kwi **{{operator.jurisdiction}}**, kwaye umthetho we {{operator.jurisdiction}} usebenza ekuqhutyweni kwedata yakho yobuqu kule seva.

## Iinguqu

Singahlaziya le migaqo ngamaxesha ngamaxesha. Iinguqu ezibalulekileyo ziya kubhengezwa phambi kokuba zisebenze. Ukuqhubeka usebenzise inkonzo emva kotshintsho kuthetha ukuba uyayamkela imigaqo ehlaziyiweyo.

## Qhagamshelana

Ngawo nawuphi na umbuzo wobumfihlo okanye ukwenza isicelo: **{{operator.email}}**
