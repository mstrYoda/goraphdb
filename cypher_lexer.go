package graphdb

import (
	"fmt"
	"strings"
	"unicode"
)

// --------------------------------------------------------------------------
// Cypher Lexer — tokenises a Cypher query string into a stream of tokens.
// --------------------------------------------------------------------------

// TokenKind identifies the type of a lexer token.
type TokenKind int

const (
	// Special
	tokEOF TokenKind = iota
	tokError

	// Literals
	tokIdent  // unquoted identifier: a, n, myVar
	tokString // 'Alice' or "Alice"
	tokInt    // 42
	tokFloat  // 3.14

	// Keywords (case-insensitive)
	tokMatch
	tokWhere
	tokReturn
	tokOrder
	tokBy
	tokLimit
	tokAnd
	tokOr
	tokNot
	tokAs
	tokAsc
	tokDesc
	tokTrue
	tokFalse
	tokNull
	tokType // type() function keyword

	// Operators
	tokEq  // =
	tokNeq // <>
	tokLt  // <
	tokGt  // >
	tokLte // <=
	tokGte // >=

	// Punctuation
	tokLParen   // (
	tokRParen   // )
	tokLBracket // [
	tokRBracket // ]
	tokLBrace   // {
	tokRBrace   // }
	tokColon    // :
	tokDot      // .
	tokDotDot   // ..
	tokComma    // ,
	tokStar     // *
	tokDash     // -
	tokArrow    // ->
	tokLArrow   // <-  (for incoming edges: <-[...]-  )
	tokParam    // $paramName
)

// Token is a single lexer token with its kind, literal text, and position.
type Token struct {
	Kind TokenKind
	Text string // raw text of the token
	Pos  int    // byte offset in the input
}

func (t Token) String() string {
	return fmt.Sprintf("%s(%q)@%d", tokenKindName(t.Kind), t.Text, t.Pos)
}

// tokenKindName returns a human-readable name for a token kind.
func tokenKindName(k TokenKind) string {
	switch k {
	case tokEOF:
		return "EOF"
	case tokError:
		return "ERROR"
	case tokIdent:
		return "IDENT"
	case tokString:
		return "STRING"
	case tokInt:
		return "INT"
	case tokFloat:
		return "FLOAT"
	case tokMatch:
		return "MATCH"
	case tokWhere:
		return "WHERE"
	case tokReturn:
		return "RETURN"
	case tokOrder:
		return "ORDER"
	case tokBy:
		return "BY"
	case tokLimit:
		return "LIMIT"
	case tokAnd:
		return "AND"
	case tokOr:
		return "OR"
	case tokNot:
		return "NOT"
	case tokAs:
		return "AS"
	case tokAsc:
		return "ASC"
	case tokDesc:
		return "DESC"
	case tokTrue:
		return "TRUE"
	case tokFalse:
		return "FALSE"
	case tokNull:
		return "NULL"
	case tokType:
		return "TYPE"
	case tokEq:
		return "="
	case tokNeq:
		return "<>"
	case tokLt:
		return "<"
	case tokGt:
		return ">"
	case tokLte:
		return "<="
	case tokGte:
		return ">="
	case tokLParen:
		return "("
	case tokRParen:
		return ")"
	case tokLBracket:
		return "["
	case tokRBracket:
		return "]"
	case tokLBrace:
		return "{"
	case tokRBrace:
		return "}"
	case tokColon:
		return ":"
	case tokDot:
		return "."
	case tokDotDot:
		return ".."
	case tokComma:
		return ","
	case tokStar:
		return "*"
	case tokDash:
		return "-"
	case tokArrow:
		return "->"
	case tokLArrow:
		return "<-"
	case tokParam:
		return "PARAM"
	default:
		return "???"
	}
}

// keywords maps uppercase keyword text to token kind.
var keywords = map[string]TokenKind{
	"MATCH":  tokMatch,
	"WHERE":  tokWhere,
	"RETURN": tokReturn,
	"ORDER":  tokOrder,
	"BY":     tokBy,
	"LIMIT":  tokLimit,
	"AND":    tokAnd,
	"OR":     tokOr,
	"NOT":    tokNot,
	"AS":     tokAs,
	"ASC":    tokAsc,
	"DESC":   tokDesc,
	"TRUE":   tokTrue,
	"FALSE":  tokFalse,
	"NULL":   tokNull,
	"TYPE":   tokType,
}

// lexer holds the state for tokenising a Cypher string.
type lexer struct {
	input  string
	pos    int
	tokens []Token
}

// tokenize converts a Cypher query string into a slice of tokens.
func tokenize(input string) ([]Token, error) {
	l := &lexer{input: input}
	if err := l.scan(); err != nil {
		return nil, err
	}
	return l.tokens, nil
}

func (l *lexer) scan() error {
	for l.pos < len(l.input) {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			break
		}

		ch := l.input[l.pos]

		switch {
		case ch == '(':
			l.emit(tokLParen, "(")
		case ch == ')':
			l.emit(tokRParen, ")")
		case ch == '[':
			l.emit(tokLBracket, "[")
		case ch == ']':
			l.emit(tokRBracket, "]")
		case ch == '{':
			l.emit(tokLBrace, "{")
		case ch == '}':
			l.emit(tokRBrace, "}")
		case ch == ':':
			l.emit(tokColon, ":")
		case ch == ',':
			l.emit(tokComma, ",")
		case ch == '*':
			l.emit(tokStar, "*")

		case ch == '.':
			// .. or .
			if l.peek(1) == '.' {
				l.emitN(tokDotDot, "..", 2)
			} else {
				l.emit(tokDot, ".")
			}

		case ch == '-':
			// -> or -
			if l.peek(1) == '>' {
				l.emitN(tokArrow, "->", 2)
			} else {
				l.emit(tokDash, "-")
			}

		case ch == '<':
			// <- or <= or <> or <
			next := l.peek(1)
			switch next {
			case '-':
				l.emitN(tokLArrow, "<-", 2)
			case '=':
				l.emitN(tokLte, "<=", 2)
			case '>':
				l.emitN(tokNeq, "<>", 2)
			default:
				l.emit(tokLt, "<")
			}

		case ch == '>':
			// >= or >
			if l.peek(1) == '=' {
				l.emitN(tokGte, ">=", 2)
			} else {
				l.emit(tokGt, ">")
			}

		case ch == '=':
			l.emit(tokEq, "=")

		case ch == '\'' || ch == '"':
			if err := l.scanString(ch); err != nil {
				return err
			}

		case isDigit(ch):
			l.scanNumber()

		case ch == '$':
			l.scanParam()

		case isIdentStart(ch):
			l.scanIdentOrKeyword()

		default:
			return fmt.Errorf("cypher lexer: unexpected character %q at position %d", ch, l.pos)
		}
	}

	l.tokens = append(l.tokens, Token{Kind: tokEOF, Text: "", Pos: l.pos})
	return nil
}

// emit adds a single-char token and advances.
func (l *lexer) emit(kind TokenKind, text string) {
	l.tokens = append(l.tokens, Token{Kind: kind, Text: text, Pos: l.pos})
	l.pos++
}

// emitN adds a multi-char token and advances by n.
func (l *lexer) emitN(kind TokenKind, text string, n int) {
	l.tokens = append(l.tokens, Token{Kind: kind, Text: text, Pos: l.pos})
	l.pos += n
}

// peek returns the byte at pos+offset, or 0 if out of bounds.
func (l *lexer) peek(offset int) byte {
	idx := l.pos + offset
	if idx >= len(l.input) {
		return 0
	}
	return l.input[idx]
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.input) && isWhitespace(l.input[l.pos]) {
		l.pos++
	}
}

// scanString scans a single-quoted or double-quoted string literal.
func (l *lexer) scanString(quote byte) error {
	start := l.pos
	l.pos++ // skip opening quote
	var b strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\\' && l.pos+1 < len(l.input) {
			// Simple escape sequences.
			l.pos++
			switch l.input[l.pos] {
			case '\\':
				b.WriteByte('\\')
			case '\'':
				b.WriteByte('\'')
			case '"':
				b.WriteByte('"')
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			default:
				b.WriteByte('\\')
				b.WriteByte(l.input[l.pos])
			}
			l.pos++
			continue
		}
		if ch == quote {
			l.pos++ // skip closing quote
			l.tokens = append(l.tokens, Token{Kind: tokString, Text: b.String(), Pos: start})
			return nil
		}
		b.WriteByte(ch)
		l.pos++
	}
	return fmt.Errorf("cypher lexer: unterminated string starting at position %d", start)
}

// scanNumber scans an integer or float literal.
func (l *lexer) scanNumber() {
	start := l.pos
	isFloat := false
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '.' && !isFloat && l.peek(1) != '.' {
			// A single dot followed by a digit is a decimal point.
			// But ".." is the range operator, so don't consume it.
			if l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1]) {
				isFloat = true
				l.pos++
				continue
			}
			break
		}
		if !isDigit(ch) {
			break
		}
		l.pos++
	}
	text := l.input[start:l.pos]
	if isFloat {
		l.tokens = append(l.tokens, Token{Kind: tokFloat, Text: text, Pos: start})
	} else {
		l.tokens = append(l.tokens, Token{Kind: tokInt, Text: text, Pos: start})
	}
}

// scanIdentOrKeyword scans an identifier and promotes it to a keyword if it matches.
func (l *lexer) scanIdentOrKeyword() {
	start := l.pos
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	text := l.input[start:l.pos]
	upper := strings.ToUpper(text)
	if kind, ok := keywords[upper]; ok {
		l.tokens = append(l.tokens, Token{Kind: kind, Text: text, Pos: start})
	} else {
		l.tokens = append(l.tokens, Token{Kind: tokIdent, Text: text, Pos: start})
	}
}

// scanParam scans a parameter reference: $name
func (l *lexer) scanParam() {
	start := l.pos
	l.pos++ // skip '$'
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	// Text stores just the name without the '$' prefix.
	text := l.input[start+1 : l.pos]
	l.tokens = append(l.tokens, Token{Kind: tokParam, Text: text, Pos: start})
}

// Character classification helpers.

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
		unicode.IsLetter(rune(ch))
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || isDigit(ch)
}
