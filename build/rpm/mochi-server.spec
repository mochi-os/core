Name:           mochi-server
Version:        %{_version}
Release:        1%{?dist}
Summary:        The distributed social operating system
License:        Proprietary
URL:            https://mochi-os.org

%description
Mochi is a decentralized social platform. This package contains the server.

%install
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}/usr/sbin
mkdir -p %{buildroot}/etc/mochi
mkdir -p %{buildroot}/var/cache/mochi
mkdir -p %{buildroot}/var/lib/mochi
mkdir -p %{buildroot}/usr/lib/systemd/system
mkdir -p %{buildroot}/usr/share/man/man1
mkdir -p %{buildroot}/usr/share/bash-completion/completions
mkdir -p %{buildroot}/usr/share/zsh/site-functions
cp %{_sourcedir}/mochi-server %{buildroot}/usr/sbin/
cp %{_sourcedir}/mochictl %{buildroot}/usr/bin/
cp %{_sourcedir}/mochi.conf %{buildroot}/etc/mochi/
cp %{_sourcedir}/mochi-server.service %{buildroot}/usr/lib/systemd/system/
cp %{_sourcedir}/mochictl.1 %{buildroot}/usr/share/man/man1/
cp %{_sourcedir}/mochictl.bash %{buildroot}/usr/share/bash-completion/completions/mochictl
cp %{_sourcedir}/_mochictl %{buildroot}/usr/share/zsh/site-functions/_mochictl

%files
%attr(755, root, root) /usr/sbin/mochi-server
%attr(755, root, root) /usr/bin/mochictl
%config(noreplace) /etc/mochi/mochi.conf
%dir /var/cache/mochi
%dir /var/lib/mochi
/usr/lib/systemd/system/mochi-server.service
/usr/share/man/man1/mochictl.1*
/usr/share/bash-completion/completions/mochictl
/usr/share/zsh/site-functions/_mochictl

%pre
if ! getent group mochi >/dev/null; then
    groupadd --system mochi
fi
if ! getent passwd mochi >/dev/null; then
    useradd --system --no-create-home --home-dir /var/lib/mochi --shell /usr/sbin/nologin --gid mochi --comment "Mochi server" mochi
fi

%post
chown -R mochi:mochi /var/lib/mochi
chown -R mochi:mochi /var/cache/mochi
systemctl daemon-reload

%preun
if [ $1 -eq 0 ]; then
    systemctl stop mochi-server 2>/dev/null || true
    systemctl disable mochi-server 2>/dev/null || true
fi

%postun
systemctl daemon-reload
