# Soukromí Mochi

Tato zásada ochrany soukromí vysvětluje, jak **{{operator.name}}** (provozovatel tohoto serveru Mochi) shromažďuje, používá a sdílí vaše osobní údaje, když si vytvoříte účet nebo službu používáte. {{operator.name}} je správcem osobních údajů.

## Co shromažďujeme

Když se zaregistrujete a používáte tento server, shromažďujeme:

- **Údaje o účtu**: uživatelské jméno, autentizační metody, které si zvolíte (e-mailovou adresu, passkeys, autentizační aplikaci, kódy pro obnovení nebo účet od poskytovatele přihlášení třetí strany), a jakékoli profilové informace, které přidáte (zobrazované jméno, avatar, životopis).
- **Obsah, který vytvoříte**: příspěvky, zprávy, komentáře, soubory a další data, která se rozhodnete prostřednictvím aplikací nainstalovaných na tomto serveru zveřejnit.
- **Obsah od jiných lidí**: příspěvky, zprávy a profilové údaje lidí na jiných serverech Mochi a v jiných sítích, které se na tento server dostanou, protože je vy nebo jiní uživatelé sledujete, odebíráte nebo si s nimi dopisujete.
- **Hlášení**: hlášení, které podáte na někoho nebo které někdo podá na vás, na Mochi nebo z jiné sítě, uchováváme, aby podle něj mohli správci tohoto serveru jednat.
- **Technické údaje**: IP adresu, informace o prohlížeči/zařízení a časová razítka požadavků, které provádíte. Tyto údaje slouží k zabezpečení a provozu služby a k odhalování zneužití.
- **Cookies**: při přihlášení je nastaven cookie relace. Ukládá pouze neprůhledný identifikátor relace; cookies nepoužíváme pro reklamu ani pro sledování napříč weby.

Vaše osobní údaje neprodáváme a nesdílíme je s inzerenty.

## Jak Mochi nakládá s vašimi daty (to je důležité)

Mochi je **peer-to-peer (P2P) síť**, nikoli platforma s jediným serverem. To mění způsob, jakým vaše data putují, a to způsoby, které jsou důležité pro vaše soukromí:

- Obsah, který zveřejníte sledujícím, odběratelům nebo ve sdílených prostorech, je **replikován na další servery Mochi**, jež hostují tyto sledující nebo odběratele. {{operator.name}} nemůže smazat kopie vašeho obsahu ze serverů, které neprovozuje. Pokud příspěvek smažete, tento server odešle oznámení o smazání partnerským uzlům, kterým příspěvek dříve odeslal, ale smazání probíhá v režimu nejlepšího úsilí a závisí na spolupráci těchto uzlů.
- Vaše **identita (uživatelské jméno, profil, veřejné klíče)** je zveřejněna v adresáři Mochi, pokud zvolíte veřejný profil, aby vás ostatní uživatelé Mochi mohli najít a kontaktovat.
- Přímý, soukromý obsah (koncepty, soukromé poznámky, soukromé zprávy mezi konkrétními příjemci) zůstává pouze na tomto serveru a na serverech příjemců; není součástí veřejné P2P sítě.
- Pokud vám tento server umožňuje **připojit se k jiným sítím** – propojením účtu, který máte u jiné služby, nebo hostováním účtu zde, který mohou lidé v jiných sítích sledovat nebo mu posílat zprávy – putuje to, co prostřednictvím tohoto účtu zveřejníte nebo odešlete, na servery v těchto sítích. Nejsou to servery Mochi a {{operator.name}} je neprovozuje. Takový účet je ve své síti veřejný a v některých sítích jsou veřejné také účty, které sledujete, příspěvky, které se vám líbí, a na Bluesky lidé, které blokujete. Pozdější úprava nebo smazání čehokoli odešle žádost, kterou tyto servery mohou ignorovat.
- Lidé, se kterými chatujete, na Mochi i v jiných sítích, mohou vidět, kdy jste online. Můžete to vypnout pomocí **Zobrazovat, kdy jsem online** v nastavení aplikace Chat.
- Hlášení, které podáte na někoho na jiném serveru, může být odesláno také moderátorům daného serveru, jako hlášení tohoto serveru, nikoli vaše: neuvádí, kdo je podal.

Stručně řečeno: cokoli, co se rozhodnete sdílet s ostatními lidmi, na Mochi nebo v jiných sítích, ze své podstaty putuje mimo kontrolu {{operator.name}}. K obsahu, který zveřejňujete, přistupujte stejně, jako byste přistupovali k e-mailu nebo jiné federované komunikaci.

## Třetí strany

S následujícími třetími stranami sdílíme jen nezbytné minimum osobních údajů, když používáte odpovídající funkce:

- **E-mailová služba**: odchozí e-maily (přihlašovací kódy, ověření adresy, oznámení) jsou odesílány prostřednictvím poskytovatele SMTP nakonfigurovaného {{operator.name}}.
- **Poskytovatelé přihlášení třetí strany**: pokud se přihlásíte přes Google, GitHub, Microsoft, Facebook nebo jiného poskytovatele OAuth, tento poskytovatel se dozví, že jste se přihlásili k tomuto serveru, a sdílí s námi vaši e-mailovou adresu a základní profilové údaje.
- **Poskytovatelé plateb**: pokud provedete nákup prostřednictvím tržištní aplikace, platba je zpracována prostřednictvím Stripe (nebo poskytovatele plateb, kterého {{operator.name}} nakonfiguroval). Plné údaje o platebních kartách neukládáme.
- **Služby push oznámení**: oznámení pro aplikaci Mochi pro Android jsou doručována prostřednictvím služby Firebase Cloud Messaging od společnosti Google, která přenáší jejich nadpis a text. Pokud se k tomuto serveru přihlásíte jinou aplikací, například aplikací pro Mastodon, Matrix nebo XMPP, její oznámení jsou doručována prostřednictvím push služby, kterou tato aplikace používá, a mohou obsahovat zprávu, které se oznámení týká.
- **Certifikační autorita**: pokud tento server hostuje účty Bluesky pod vlastní doménou, je pro adresu každého účtu vydán bezpečnostní certifikát. Certifikační autority zveřejňují každý certifikát, který vydají, ve veřejných protokolech, takže jsou tam tyto adresy uvedeny.
- **Jiné sítě**: pokud propojíte účet, který máte u jiné služby, tato služba obdrží to, co jeho prostřednictvím zveřejníte, a tento server ukládá přihlašovací údaje, které mu umožňují tam jednat vaším jménem. Pokud zde hostujete účet pro jinou síť, servery a služby této sítě obdrží to, co do ní zveřejníte, spolu s veřejnou adresou účtu.

## Vaše práva

V závislosti na tom, kde žijete, můžete mít právo na přístup ke svým osobním údajům, jejich opravu, export, omezení zpracování nebo smazání. Svá data můžete sami exportovat a svůj účet zrušit v Nastavení. Zrušení účtu jej uzamkne na ochrannou lhůtu, během níž se můžete přihlásit a zrušení odvolat; poté jsou účet a jeho data z tohoto serveru odstraněny (s výhradou doby uchovávání záloh a jakékoli zákonné povinnosti uchovávat konkrétní záznamy). Pro uplatnění jakéhokoli jiného práva, nebo pokud se nemůžete přihlásit, pište na **{{operator.email}}**.

## Doba uchovávání údajů

Údaje o vašem účtu uchováváme po celou dobu, kdy je váš účet aktivní, a po přiměřenou dobu po jeho zrušení, abychom mohli obnovit data v případě náhodného smazání a splnili zákonné a auditní povinnosti. Zálohy obvykle uchováváme až po dobu 90 dnů.

## Kde se nacházíme

Tato služba je provozována z **{{operator.jurisdiction}}** a na zpracování vašich osobních údajů na tomto serveru se vztahuje právo {{operator.jurisdiction}}.

## Změny

Tyto podmínky můžeme čas od času aktualizovat. Podstatné změny budou oznámeny předtím, než vstoupí v platnost. Pokračováním v používání služby po změně vyjadřujete souhlas s aktualizovanými podmínkami.

## Kontakt

Pro jakýkoli dotaz týkající se soukromí nebo pro podání žádosti: **{{operator.email}}**
