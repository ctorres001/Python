import pandas as pd
import re
import numpy as np
import difflib
import os

# Librerías externas
from email_validator import validate_email, EmailNotValidError
import dns.resolver


class EmailValidator:
    def __init__(self):
        self.exact_match_keywords = {
            'usuario': ['demo', 'fake', 'falso', 'test', 'cuenta'],
            'dominio': ['demo', 'fake', 'falso', 'test', 'cuenta']
        }
        self.partial_match_keywords = {
            'usuario': ['tiene', 'nocuenta', 'correo', 'brinda', 'aplica',
                        'ninguno', 'ejemplo', 'temporal', 'noexiste', 'aaaa'],
            'dominio': ['tiene', 'nocuenta', 'correo', 'brinda', 'aplica',
                        'ninguno', 'ejemplo', 'sinmail', 'sincorreo', 'noaplica']
        }
        self.common_domains = [
            'gmail.com', 'hotmail.com', 'yahoo.com', 'outlook.com',
            'live.com', 'icloud.com', 'aol.com', 'msn.com',
            'terra.com.pe', 'hotmail.es', 'yahoo.es', 'correo.pe',
            'uni.pe', 'pucp.edu.pe', 'unmsm.edu.pe'
        ]
        self.email_pattern = re.compile(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )

    def clean_email(self, email):
        if pd.isna(email) or email == '':
            return None
        email = str(email).strip().lower()
        email = re.sub(r'[^\w@.-]', '', email)
        return email if email else None

    def validate_syntax(self, email):
        if not email:
            return False, "Email vacío o nulo"
        if not self.email_pattern.match(email):
            return False, "Formato de email inválido"
        if email.count('@') != 1:
            return False, "Debe contener exactamente un símbolo @"
        try:
            user, domain = email.split('@')
        except ValueError:
            return False, "No se puede separar usuario y dominio"
        if len(user) < 4:
            return False, "Usuario debe tener mínimo 4 caracteres"
        if len(user) > 64:
            return False, "Usuario muy largo (máximo 64 caracteres)"
        if len(domain) < 4:
            return False, "Dominio debe tener mínimo 4 caracteres"
        if len(domain) > 253:
            return False, "Dominio muy largo (máximo 253 caracteres)"
        if '.' not in domain:
            return False, "Dominio sin extensión válida"
        if len(domain.split('.')[-1]) < 2:
            return False, "Extensión de dominio debe tener mínimo 2 caracteres"
        if domain.startswith('.') or domain.endswith('.') or domain.startswith('-') or domain.endswith('-'):
            return False, "Dominio no puede empezar o terminar con punto o guión"
        return True, "Sintaxis válida"

    def check_invalid_keywords(self, email):
        if not email:
            return True, "Email vacío"
        user, domain = email.split('@')
        for keyword in self.exact_match_keywords['usuario']:
            if re.search(r'\b' + re.escape(keyword) + r'\b', user.lower()):
                return True, f"Palabra clave inválida exacta en usuario: '{keyword}'"
        for keyword in self.partial_match_keywords['usuario']:
            if keyword in user.lower():
                return True, f"Palabra clave inválida en usuario: '{keyword}'"
        for keyword in self.exact_match_keywords['dominio']:
            if re.search(r'\b' + re.escape(keyword) + r'\b', domain.lower()):
                return True, f"Palabra clave inválida exacta en dominio: '{keyword}'"
        for keyword in self.partial_match_keywords['dominio']:
            if keyword in domain.lower():
                return True, f"Palabra clave inválida en dominio: '{keyword}'"
        return False, "Sin palabras clave inválidas"

    def suggest_domain_correction(self, domain):
        matches = difflib.get_close_matches(domain.lower(), self.common_domains, n=1, cutoff=0.7)
        return matches[0] if matches and matches[0] != domain.lower() else None

    def validate_domain_typos(self, email):
        if not email or '@' not in email:
            return False, "Email inválido", None
        _, domain = email.split('@')
        suggested_domain = self.suggest_domain_correction(domain)
        if suggested_domain:
            return False, "Posible error tipográfico en dominio", suggested_domain
        return True, "Dominio parece correcto", None

    def validate_with_library(self, email):
        try:
            valid = validate_email(email, check_deliverability=False)
            return True, "Validación con email-validator correcta", valid.email
        except EmailNotValidError as e:
            return False, f"Error con email-validator: {str(e)}", None

    def validate_domain_dns(self, domain):
        """Revisa si el dominio tiene MX o al menos A. No bloquea en caso de error."""
        try:
            answers = dns.resolver.resolve(domain, 'MX')
            if answers:
                return True, "Dominio con registros MX válidos"
        except Exception:
            try:
                answers = dns.resolver.resolve(domain, 'A')
                if answers:
                    return True, "Dominio con registro A válido (sin MX explícito)"
            except Exception as e:
                return False, f"Error DNS: {str(e)}"
        return False, "Dominio sin registros MX ni A"

    def validate_email_complete(self, email):
        result = {
            'email_original': email,
            'email_limpio': None,
            'estado': 'SIN CORREO',
            'errores': [],
            'dominio_sugerido': None,
            'email_corregido': None
        }

        if pd.isna(email) or email == '' or str(email).strip() == '':
            result['estado'] = 'SIN CORREO'
            result['errores'].append("Email vacío o nulo")
            return result

        clean_email = self.clean_email(email)
        result['email_limpio'] = clean_email
        if not clean_email:
            result['estado'] = 'SIN CORREO'
            result['errores'].append("Email vacío después de limpieza")
            return result

        result['estado'] = 'INCORRECTO'

        syntax_valid, syntax_msg = self.validate_syntax(clean_email)
        if not syntax_valid:
            result['errores'].append(syntax_msg)
            return result

        has_invalid_keywords, keyword_msg = self.check_invalid_keywords(clean_email)
        if has_invalid_keywords:
            result['errores'].append(keyword_msg)
            return result

        domain_valid, domain_msg, suggested_domain = self.validate_domain_typos(clean_email)
        if not domain_valid:
            result['errores'].append(domain_msg)
            result['dominio_sugerido'] = suggested_domain
            if suggested_domain:
                user, _ = clean_email.split('@')
                result['email_corregido'] = f"{user}@{suggested_domain}"
            return result

        lib_ok, lib_msg, _ = self.validate_with_library(clean_email)
        if not lib_ok:
            result['errores'].append(lib_msg)
            return result

        domain = clean_email.split('@')[1]
        dns_ok, dns_msg = self.validate_domain_dns(domain)
        if not dns_ok:
            result['errores'].append(f"Advertencia DNS: {dns_msg}")
        else:
            result['errores'].append(dns_msg)

        # ✅ Siempre CORRECTO si pasó regex + email-validator + keywords
        result['estado'] = 'CORRECTO'
        return result


def process_email_database(input_file, output_file):
    print("Iniciando validación de correos electrónicos...")
    try:
        df = pd.read_excel(input_file)
        print(f"Se cargaron {len(df)} registros")
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return

    email_columns = [col for col in df.columns if any(k in col.lower() for k in ['email', 'correo', 'mail', '@'])]
    if not email_columns:
        for col in df.columns:
            sample_values = df[col].dropna().astype(str).head(10)
            if any('@' in str(val) for val in sample_values):
                email_columns.append(col)
                break
    if not email_columns:
        print("No se encontraron columnas con emails")
        return
    print(f"Columnas de email detectadas: {email_columns}")

    validator = EmailValidator()
    for email_col in email_columns:
        print(f"\nProcesando columna: {email_col}")
        base_name = email_col.replace(' ', '_').lower()
        df[f'{base_name}_limpio'] = ''
        df[f'{base_name}_estado'] = 'SIN CORREO'
        df[f'{base_name}_errores'] = ''
        df[f'{base_name}_dominio_sugerido'] = ''
        df[f'{base_name}_corregido'] = ''

        for idx, email in enumerate(df[email_col]):
            if idx % 1000 == 0:
                print(f"Procesando registro {idx+1}/{len(df)}")
            result = validator.validate_email_complete(email)
            df.at[idx, f'{base_name}_limpio'] = result['email_limpio'] or ''
            df.at[idx, f'{base_name}_estado'] = result['estado']
            df.at[idx, f'{base_name}_errores'] = '; '.join(result['errores'])
            df.at[idx, f'{base_name}_dominio_sugerido'] = result['dominio_sugerido'] or ''
            df.at[idx, f'{base_name}_corregido'] = result['email_corregido'] or ''

    # Guardar resultados
    try:
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        if os.path.exists(output_file):
            os.remove(output_file)

        with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
            df.to_excel(writer, sheet_name='Datos_Completos', index=False)

        print(f"\nArchivo guardado exitosamente: {output_file}")
        print(f"Tamaño del archivo: {os.path.getsize(output_file):,} bytes")

    except PermissionError:
        print("❌ Error: El archivo está abierto en Excel. Ciérralo e intenta nuevamente.")
    except Exception as e:
        print(f"❌ Error al guardar el archivo: {e}")


if __name__ == "__main__":
    input_path = r"D:\FNB\Reportes\19. Reportes IBR\09. Reporte Validez Correos\Base\Base.xlsx"
    output_path = r"D:\FNB\Reportes\19. Reportes IBR\09. Reporte Validez Correos\Base\BaseRevisada.xlsx"
    process_email_database(input_path, output_path)
