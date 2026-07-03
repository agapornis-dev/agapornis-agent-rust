use super::*;

pub fn authorize_master(request: Request<()>) -> Result<Request<()>, Status> {
    use x509_parser::{extensions::ParsedExtension, prelude::FromDer};
    let certs = request
        .peer_certs()
        .ok_or_else(|| Status::unauthenticated("mTLS client certificate is required"))?;
    let der = certs
        .first()
        .ok_or_else(|| Status::unauthenticated("mTLS client certificate is required"))?;
    let (_, cert) = x509_parser::certificate::X509Certificate::from_der(der.as_ref())
        .map_err(|_| Status::unauthenticated("invalid client certificate"))?;
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|v| v.as_str().ok())
        .unwrap_or("");
    if cn != "agapornis-master" {
        return Err(Status::permission_denied(
            "client certificate is not authorized",
        ));
    }
    let mut client_auth = false;
    for ext in cert.extensions() {
        if let ParsedExtension::ExtendedKeyUsage(eku) = ext.parsed_extension() {
            client_auth = eku.client_auth;
        }
    }
    if !client_auth {
        return Err(Status::permission_denied(
            "client certificate is missing clientAuth usage",
        ));
    }
    Ok(request)
}
