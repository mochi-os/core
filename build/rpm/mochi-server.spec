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
mkdir -p %{buildroot}/etc/mochi
mkdir -p %{buildroot}/var/cache/mochi
mkdir -p %{buildroot}/var/lib/mochi
mkdir -p %{buildroot}/usr/lib/systemd/system
cp %{_sourcedir}/mochi-server %{buildroot}/usr/bin/
cp %{_sourcedir}/mochi.conf %{buildroot}/etc/mochi/
cp %{_sourcedir}/mochi-server.service %{buildroot}/usr/lib/systemd/system/

%files
%attr(755, root, root) /usr/bin/mochi-server
%config(noreplace) /etc/mochi/mochi.conf
%dir /var/cache/mochi
%dir /var/lib/mochi
/usr/lib/systemd/system/mochi-server.service

%post
systemctl daemon-reload

%preun
if [ $1 -eq 0 ]; then
    systemctl stop mochi-server 2>/dev/null || true
    systemctl disable mochi-server 2>/dev/null || true
fi

%postun
systemctl daemon-reload
