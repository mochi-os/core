# Mochi Integritet

Denna integritetsinformation förklarar hur **{{operator.name}}** (operatören av denna Mochi-server) samlar in, använder och delar dina personuppgifter när du skapar ett konto eller använder tjänsten. {{operator.name}} är personuppgiftsansvarig.

## Vad vi samlar in

När du registrerar dig och använder denna server samlar vi in:

- **Kontouppgifter**: ett användarnamn, de autentiseringsmetoder du väljer (e-postadress, passkeys, autentiseringsapp, återställningskoder eller ett konto från en inloggningsleverantör från tredje part) samt all profilinformation du lägger till (visningsnamn, avatar, biografi).
- **Innehåll du skapar**: inlägg, meddelanden, kommentarer, filer och andra uppgifter som du väljer att publicera via de appar som är installerade på denna server.
- **Innehåll från andra personer**: inlägg, meddelanden och profiluppgifter för personer på andra Mochi-servrar och i andra nätverk, som når denna server eftersom du eller andra användare följer dem, prenumererar på dem eller brevväxlar med dem.
- **Anmälningar**: en anmälan som du gör om någon, eller som någon gör om dig, på Mochi eller från ett annat nätverk, sparas så att administratörerna för denna server kan agera på den.
- **Tekniska uppgifter**: IP-adress, webbläsar-/enhetsinformation och tidsstämplar för de förfrågningar du gör. Detta används för att hålla tjänsten säker och i drift och för att upptäcka missbruk.
- **Cookies**: en sessionscookie sätts när du loggar in. Den lagrar endast en ogenomskinlig sessionsidentifierare; vi använder inte cookies för annonsering eller spårning över webbplatser.

Vi säljer inte dina personuppgifter och vi delar dem inte med annonsörer.

## Hur Mochi hanterar dina uppgifter (detta är viktigt)

Mochi är ett **peer-to-peer-nätverk (P2P)**, inte en plattform med en enskild server. Det förändrar hur dina uppgifter flödar på sätt som har betydelse för din integritet:

- Innehåll som du publicerar till följare, prenumeranter eller delade utrymmen **replikeras till andra Mochi-servrar** som värd för dessa följare eller prenumeranter. {{operator.name}} kan inte ta bort kopior av ditt innehåll från servrar som vi inte driver. Om du tar bort ett inlägg kommer denna server att skicka ett borttagningsmeddelande till de peers som inlägget tidigare har skickats till, men borttagningen sker enligt bästa förmåga och beror på samarbetet med dessa peers.
- Din **identitet (användarnamn, profil, publika nycklar)** publiceras i Mochi-katalogen om du väljer en publik profil, så att andra Mochi-användare kan hitta och kontakta dig.
- Direkt, privat innehåll (utkast, privata anteckningar, privata meddelanden mellan specifika mottagare) stannar endast på denna server och mottagarnas servrar; det är inte en del av det publika P2P-nätverket.
- Där denna server låter dig **ansluta till andra nätverk** - genom att koppla ett konto du har hos en annan tjänst, eller genom ett konto som denna server är värd för och som personer i andra nätverk kan följa eller skicka meddelanden till - går det du publicerar eller skickar via det kontot till servrar i dessa nätverk. De är inte Mochi-servrar och {{operator.name}} driver dem inte. Ett sådant konto är publikt i sitt nätverk, och i vissa nätverk är även de konton du följer, de inlägg du gillar och, på Bluesky, de personer du blockerar publika. Att redigera eller ta bort något i efterhand skickar en begäran som dessa servrar kan ignorera.
- Personer du chattar med, på Mochi och i andra nätverk, kan se när du är online. Du kan stänga av detta med **Visa när jag är tillgänglig** i chattappens inställningar.
- En anmälan som du gör om någon på en annan server kan även skickas till den serverns moderatorer, som denna servers anmälan och inte din: den anger inte vem som gjorde den.

Kort sagt: allt du väljer att dela med andra människor, på Mochi eller i andra nätverk, kommer att färdas bortom {{operator.name}}s kontroll, av designprincip. Behandla innehåll du publicerar som du skulle behandla e-post eller någon annan federerad kommunikation.

## Tredje parter

Vi delar minimum av personuppgifter som krävs med följande tredje parter när du använder de relaterade funktionerna:

- **E-posttjänst**: utgående e-post (inloggningskoder, adressverifiering, notiser) skickas via en SMTP-leverantör som konfigurerats av {{operator.name}}.
- **Inloggningsleverantörer från tredje part**: om du loggar in med Google, GitHub, Microsoft, Facebook eller en annan OAuth-leverantör får den leverantören veta att du loggade in på denna server och delar din e-postadress och grundläggande profiluppgifter med oss.
- **Betalleverantörer**: om du gör ett köp via en marknadsplatsapp behandlas betalningen av Stripe (eller den betalleverantör som {{operator.name}} har konfigurerat). Vi lagrar inte fullständiga kortuppgifter.
- **Tjänster för push-notiser**: notiser till Mochi-appen på Android levereras via Googles Firebase Cloud Messaging, som överför deras rubrik och text. Om du loggar in på denna server med en annan app, till exempel en app för Mastodon, Matrix eller XMPP, levereras dess notiser via den push-tjänst som appen använder och kan innehålla det meddelande som notisen gäller.
- **Certifikatutfärdare**: där denna server är värd för Bluesky-konton under sin egen domän utfärdas ett säkerhetscertifikat för varje kontos adress. Certifikatutfärdare publicerar varje certifikat de utfärdar i offentliga loggar, så dessa adresser listas där.
- **Andra nätverk**: om du kopplar ett konto du har hos en annan tjänst får den tjänsten det du publicerar via kontot, och denna server lagrar de inloggningsuppgifter som låter servern agera för din räkning där. Om du har ett konto på denna server för ett annat nätverk får det nätverkets servrar och tjänster det du publicerar i nätverket, tillsammans med kontots publika adress.

## Dina rättigheter

Beroende på var du bor kan du ha rätt att få tillgång till, korrigera, exportera, begränsa eller radera dina personuppgifter. Du kan själv exportera dina uppgifter och stänga ditt konto i Inställningar. När du stänger ditt konto låses det under en frist, då du kan logga in och avbryta stängningen; därefter tas kontot och dess uppgifter bort från denna server (med förbehåll för säkerhetskopieringsretention och eventuella rättsliga skyldigheter att bevara specifika uppgifter). För att utöva någon annan rättighet, eller om du inte kan logga in, skicka e-post till **{{operator.email}}**.

## Uppgiftslagring

Vi behåller dina kontouppgifter så länge ditt konto är aktivt och under en rimlig period efter stängning för att kunna återhämta oss från oavsiktlig borttagning och uppfylla rättsliga och revisionsmässiga skyldigheter. Säkerhetskopior bevaras vanligtvis i upp till 90 dagar.

## Var vi finns

Denna tjänst drivs från **{{operator.jurisdiction}}**, och lagen i {{operator.jurisdiction}} gäller för behandlingen av dina personuppgifter på denna server.

## Ändringar

Vi kan uppdatera dessa villkor då och då. Väsentliga ändringar kommer att tillkännages innan de träder i kraft. Fortsatt användning av tjänsten efter en ändring innebär att du accepterar de uppdaterade villkoren.

## Kontakt

För integritetsfrågor eller för att göra en begäran: **{{operator.email}}**
