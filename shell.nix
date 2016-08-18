with import <nixpkgs> {}; 
let sourceFilesOnly = path: type:
    (lib.hasPrefix "var" (toString path)) ||
    (lib.hasPrefix "target" (toString path)) ;
in stdenv.mkDerivation {
    name = "merlion";
    src = builtins.filterSource sourceFilesOnly ./.;
    buildInputs = [ leiningen openjdk 
      etcd 
      jq
      openssl
      apacheHttpd
      ];
    M2REPOSITORY = ''m2repo'';
    buildPhase = ''
      lein uberjar
    '';
    shellHook = ''
      source nix-shell-hook.sh
    '';
}

