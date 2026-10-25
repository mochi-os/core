# Privacy ng Mochi

Ipinapaliwanag ng privacy notice na ito kung paano kinokolekta, ginagamit, at ibinabahagi ng **{{operator.name}}** (ang operator ng Mochi server na ito) ang iyong personal na data kapag gumawa ka ng account o gumamit ng serbisyo. Ang {{operator.name}} ang data controller.

## Ano ang aming kinokolekta

Kapag nag-sign up ka at gumamit ng server na ito, kinokolekta namin ang:

- **Account data**: isang username, ang mga authentication method na pinili mo (email address, passkey, authenticator app, recovery code, o isang account mula sa third-party login provider), at anumang impormasyon ng profile na idinaragdag mo (display name, avatar, biography).
- **Nilalamang ginagawa mo**: mga post, mensahe, komento, file, at iba pang data na pinipili mong i-publish sa pamamagitan ng mga app na naka-install sa server na ito.
- **Nilalaman mula sa ibang tao**: mga post, mensahe, at detalye ng profile ng mga tao sa ibang Mochi server at sa ibang network, na umaabot sa server na ito dahil ikaw o ang ibang user ay nagfo-follow, nagsu-subscribe, o nakikipagsulatan sa kanila.
- **Mga report**: ang report na ginagawa mo tungkol sa isang tao, o na ginagawa ng isang tao tungkol sa iyo, sa Mochi man o mula sa ibang network, ay itinatago upang maaksyunan ito ng mga administrator ng server na ito.
- **Technical data**: IP address, impormasyon ng browser/device, at timestamp para sa mga request na ginagawa mo. Ginagamit ito upang panatilihing secure at gumagana ang serbisyo, at upang matukoy ang pang-aabuso.
- **Cookies**: isang session cookie ang itinatakda kapag nag-sign in ka. Nag-iimbak lamang ito ng opaque na session identifier; hindi kami gumagamit ng cookies para sa advertising o cross-site tracking.

Hindi namin ibinebenta ang iyong personal na data, at hindi namin ito ibinabahagi sa mga advertiser.

## Paano hinahawakan ng Mochi ang iyong data (mahalaga ito)

Ang Mochi ay isang **peer-to-peer (P2P) network**, hindi isang single-server platform. Binabago nito ang daloy ng iyong data sa mga paraang mahalaga para sa iyong privacy:

- Ang nilalamang ipino-publish mo sa mga follower, subscriber, o shared space ay **nirereplicate sa ibang Mochi server** na nagho-host sa mga follower o subscriber na iyon. Hindi maaaring burahin ng {{operator.name}} ang mga kopya ng iyong nilalaman mula sa mga server na hindi nito pinapatakbo. Kung magbu-burahin ka ng post, magpapadala ang server na ito ng deletion notice sa mga peer kung saan dating ipinadala ang post, ngunit ang pagbura ay best-effort at nakadepende sa kooperasyon ng mga peer na iyon.
- Ang iyong **identity (username, profile, public key)** ay nai-publish sa Mochi directory kung pipili ka ng public profile, upang mahanap ka at makontak ng ibang Mochi user.
- Ang direkta, pribadong nilalaman (mga draft, pribadong tala, pribadong mensahe sa pagitan ng mga partikular na tatanggap) ay nananatili lamang sa server na ito at sa mga server ng tatanggap; hindi ito bahagi ng pampublikong P2P network.
- Kung saan pinapayagan ka ng server na ito na **kumonekta sa ibang network** - sa pamamagitan ng pag-link ng account na hawak mo sa ibang serbisyo, o sa pamamagitan ng pag-host dito ng account na maaaring i-follow o padalhan ng mensahe ng mga tao sa ibang network - ang ipino-publish o ipinapadala mo sa pamamagitan ng account na iyon ay napupunta sa mga server sa mga network na iyon. Hindi sila mga Mochi server at hindi sila pinapatakbo ng {{operator.name}}. Ang ganitong account ay pampubliko sa network nito, at sa ilang network, pampubliko rin ang mga account na fino-follow mo, ang mga post na nila-like mo at, sa Bluesky, ang mga taong bina-block mo. Ang pag-edit o pagbura ng isang bagay sa ibang pagkakataon ay nagpapadala ng kahilingan na maaaring balewalain ng mga server na iyon.
- Nakikita ng mga taong kausap mo sa chat, sa Mochi at sa ibang network, kung kailan ka online. Maaari mo itong i-off gamit ang **Ipakita kapag online ako** sa mga setting ng chat app.
- Ang report na ginagawa mo tungkol sa isang tao sa ibang server ay maaari ring ipadala sa mga moderator ng server na iyon, bilang report ng server na ito at hindi sa iyo: hindi nito sinasabi kung sino ang gumawa nito.

Sa madaling salita: anumang pinipili mong i-share sa ibang tao, sa Mochi man o sa ibang network, ay maglalakbay sa labas ng kontrol ng {{operator.name}}, sa pamamagitan ng disenyo. Ituring ang nilalamang ipino-publish mo na parang email o anumang ibang federated na komunikasyon.

## Mga third party

Ibinabahagi namin ang minimum na personal na data na kinakailangan sa mga sumusunod na third party kapag ginamit mo ang mga kaugnay na feature:

- **Email service**: ang mga papalabas na email (sign-in code, address verification, notification) ay ipinapadala sa pamamagitan ng SMTP provider na na-configure ng {{operator.name}}.
- **Mga third-party login provider**: kung mag-sign in ka sa Google, GitHub, Microsoft, Facebook, o ibang OAuth provider, malalaman ng provider na iyon na nag-sign in ka sa server na ito at ibabahagi nito ang iyong email address at basic profile data sa amin.
- **Mga payment provider**: kung bibili ka sa pamamagitan ng marketplace app, ang bayad ay pinoproseso ng Stripe (o anumang payment provider na na-configure ng {{operator.name}}). Hindi namin ini-store ang buong card details.
- **Mga push notification service**: ang mga notification sa Mochi app sa Android ay inihahatid sa pamamagitan ng Firebase Cloud Messaging ng Google, na nagdadala ng kanilang pamagat at teksto. Kung mag-sign in ka sa server na ito gamit ang ibang app, gaya ng Mastodon, Matrix o XMPP app, ang mga notification nito ay inihahatid sa pamamagitan ng push service na ginagamit ng app na iyon, at maaaring kasama ang mensaheng tinutukoy ng notification.
- **Certificate authority**: kung saan nagho-host ang server na ito ng mga Bluesky account sa ilalim ng sarili nitong domain, binibigyan ng security certificate ang address ng bawat account. Inilalathala ng mga certificate authority ang bawat certificate na kanilang inilalabas sa mga pampublikong log, kaya nakalista roon ang mga address na iyon.
- **Ibang network**: kung ili-link mo ang isang account na hawak mo sa ibang serbisyo, matatanggap ng serbisyong iyon ang ipino-post mo sa pamamagitan nito, at ini-store ng server na ito ang credential na nagpapahintulot dito na kumilos para sa iyo roon. Kung magho-host ka rito ng account para sa ibang network, matatanggap ng mga server at serbisyo ng network na iyon ang ipino-publish mo sa network na iyon, kasama ang pampublikong address ng account.

## Iyong mga karapatan

Depende sa kung saan ka nakatira, maaari kang may karapatang mag-access, magtuwid, mag-export, mag-restrict, o magbura ng iyong personal na data. Maaari mong i-export ang iyong data at isara ang iyong account nang ikaw mismo sa Mga setting. Kapag isinara mo ang iyong account, mala-lock ito sa loob ng isang grace period, kung kailan maaari kang mag-sign in at kanselahin ang pagsasara; pagkatapos nito, aalisin ang account at ang data nito mula sa server na ito (napapailalim sa backup retention at anumang legal obligation na panatilihin ang partikular na records). Upang gamitin ang anumang ibang karapatan, o kung hindi ka makapag-sign in, mag-email sa **{{operator.email}}**.

## Pagpapanatili ng data

Pinapanatili namin ang iyong account data hangga't aktibo ang iyong account, at sa makatwirang panahon pagkatapos ng pagsasara upang makabawi mula sa hindi sinasadyang pagbura at upang matugunan ang legal at audit obligations. Ang mga backup ay karaniwang pinapanatili ng hanggang 90 araw.

## Saan kami nakatira

Pinapatakbo ang serbisyong ito mula sa **{{operator.jurisdiction}}**, at ang batas ng {{operator.jurisdiction}} ay nalalapat sa pagproseso ng iyong personal na data sa server na ito.

## Mga pagbabago

Maaari naming i-update ang mga tuntuning ito paminsan-minsan. Ang mga material na pagbabago ay ipapahayag bago ito magkabisa. Ang patuloy na paggamit ng serbisyo pagkatapos ng pagbabago ay nangangahulugang tinatanggap mo ang mga na-update na tuntunin.

## Pakikipag-ugnayan

Para sa anumang katanungan sa privacy o para magsumite ng kahilingan: **{{operator.email}}**
