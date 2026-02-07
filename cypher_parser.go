package graphdb

import (
	"fmt"
	"strconv"
)

// --------------------------------------------------------------------------
// Cypher Parser — recursive descent parser that turns a token stream into
// an AST (CypherQuery).
//
// Supported grammar (read-only subset):
//
//   Query       → MATCH Pattern [WHERE Expr] RETURN ReturnItems [ORDER BY OrderItems] [LIMIT int]
//   Pattern     → NodePat ( RelPat NodePat )*
//   NodePat     → '(' [ident] ['{' PropMap '}'] ')'
//   RelPat      → '-[' [ident] [':' label] ['*' [int] ['..' [int]] ] ']->'
//               |  '<-[' ... ']-'
//               |  '-[' ... ']-'
//   PropMap     → ident ':' Literal ( ',' ident ':' Literal )*
//   Expr        → OrExpr
//   OrExpr      → AndExpr ( OR AndExpr )*
//   AndExpr     → NotExpr ( AND NotExpr )*
//   NotExpr     → [NOT] Comparison
//   Comparison  → Primary [ ('=' | '<>' | '<' | '>' | '<=' | '>=') Primary ]
//   Primary     → ident '.' ident
//               |  ident '(' Expr ')'
//               |  ident
//               |  Literal
//   Literal     → STRING | INT | FLOAT | TRUE | FALSE | NULL
//   ReturnItems → ReturnItem ( ',' ReturnItem )*
//   ReturnItem  → Expr [ AS ident ]
//   OrderItems  → OrderItem ( ',' OrderItem )*
//   OrderItem   → Expr [ ASC | DESC ]
// --------------------------------------------------------------------------

// parser holds the state for parsing a token stream.
type parser struct {
	tokens []Token
	pos    int
}

// parseCypher is the entry point: tokenise + parse a Cypher query string.
func parseCypher(input string) (*CypherQuery, error) {
	tokens, err := tokenize(input)
	if err != nil {
		return nil, err
	}
	p := &parser{tokens: tokens}
	return p.parseQuery()
}

// ---------------- helpers -------------------------------------------------

// cur returns the current token.
func (p *parser) cur() Token {
	if p.pos >= len(p.tokens) {
		return Token{Kind: tokEOF}
	}
	return p.tokens[p.pos]
}

// advance moves to the next token and returns the consumed one.
func (p *parser) advance() Token {
	t := p.cur()
	p.pos++
	return t
}

// expect consumes a token of the given kind or returns an error.
func (p *parser) expect(kind TokenKind) (Token, error) {
	t := p.cur()
	if t.Kind != kind {
		return t, fmt.Errorf("cypher parser: expected %s but got %s at position %d",
			tokenKindName(kind), tokenKindName(t.Kind), t.Pos)
	}
	p.pos++
	return t, nil
}

// is checks if the current token matches the given kind.
func (p *parser) is(kind TokenKind) bool {
	return p.cur().Kind == kind
}

// match consumes the current token if it matches the kind, returning true.
func (p *parser) match(kind TokenKind) bool {
	if p.is(kind) {
		p.pos++
		return true
	}
	return false
}

// ---------------- query ---------------------------------------------------

func (p *parser) parseQuery() (*CypherQuery, error) {
	q := &CypherQuery{}

	// MATCH
	if _, err := p.expect(tokMatch); err != nil {
		return nil, err
	}
	pat, err := p.parsePattern()
	if err != nil {
		return nil, err
	}
	q.Match = MatchClause{Pattern: pat}

	// WHERE (optional)
	if p.is(tokWhere) {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		q.Where = &expr
	}

	// RETURN
	ret, err := p.parseReturnClause()
	if err != nil {
		return nil, err
	}
	q.Return = ret

	// ORDER BY (optional)
	if p.is(tokOrder) {
		p.advance()
		if _, err := p.expect(tokBy); err != nil {
			return nil, err
		}
		items, err := p.parseOrderItems()
		if err != nil {
			return nil, err
		}
		q.OrderBy = items
	}

	// LIMIT (optional)
	if p.is(tokLimit) {
		p.advance()
		tok, err := p.expect(tokInt)
		if err != nil {
			return nil, fmt.Errorf("cypher parser: LIMIT requires an integer")
		}
		n, _ := strconv.Atoi(tok.Text)
		q.Limit = n
	}

	// Should be at EOF now.
	if !p.is(tokEOF) {
		return nil, fmt.Errorf("cypher parser: unexpected token %s at position %d",
			tokenKindName(p.cur().Kind), p.cur().Pos)
	}

	return q, nil
}

// ---------------- pattern -------------------------------------------------

func (p *parser) parsePattern() (Pattern, error) {
	pat := Pattern{}

	// First node is required.
	node, err := p.parseNodePattern()
	if err != nil {
		return pat, err
	}
	pat.Nodes = append(pat.Nodes, node)

	// Optionally, parse alternating (rel, node) pairs.
	for p.is(tokDash) || p.is(tokLArrow) {
		rel, err := p.parseRelPattern()
		if err != nil {
			return pat, err
		}
		pat.Rels = append(pat.Rels, rel)

		node, err := p.parseNodePattern()
		if err != nil {
			return pat, err
		}
		pat.Nodes = append(pat.Nodes, node)
	}

	return pat, nil
}

// parseNodePattern parses: '(' [ident] [':' label]* ['{' propMap '}'] ')'
func (p *parser) parseNodePattern() (NodePattern, error) {
	np := NodePattern{}

	if _, err := p.expect(tokLParen); err != nil {
		return np, err
	}

	// Optional variable name.
	if p.is(tokIdent) {
		np.Variable = p.advance().Text
	}

	// Optional labels: :Label1:Label2
	for p.is(tokColon) {
		p.advance() // consume ':'
		if !p.is(tokIdent) {
			return np, fmt.Errorf("cypher parser: expected label name after ':' at position %d", p.cur().Pos)
		}
		np.Labels = append(np.Labels, p.advance().Text)
	}

	// Optional inline properties: { key: value, ... }
	if p.is(tokLBrace) {
		p.advance()
		props, err := p.parsePropMap()
		if err != nil {
			return np, err
		}
		np.Props = props
	}

	if _, err := p.expect(tokRParen); err != nil {
		return np, err
	}

	return np, nil
}

// parsePropMap parses: ident ':' literal (',' ident ':' literal)* '}'
func (p *parser) parsePropMap() (map[string]any, error) {
	props := make(map[string]any)

	for !p.is(tokRBrace) && !p.is(tokEOF) {
		// key
		keyTok, err := p.expect(tokIdent)
		if err != nil {
			return nil, fmt.Errorf("cypher parser: expected property key, got %s", tokenKindName(p.cur().Kind))
		}
		// :
		if _, err := p.expect(tokColon); err != nil {
			return nil, err
		}
		// value (literal)
		val, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		props[keyTok.Text] = val

		// optional comma
		p.match(tokComma)
	}

	if _, err := p.expect(tokRBrace); err != nil {
		return nil, err
	}
	return props, nil
}

// parseLiteral parses a literal value: string, int, float, true, false, null, or $param.
func (p *parser) parseLiteral() (any, error) {
	t := p.cur()
	switch t.Kind {
	case tokParam:
		p.advance()
		return paramRef(t.Text), nil
	case tokString:
		p.advance()
		return t.Text, nil
	case tokInt:
		p.advance()
		n, err := strconv.ParseInt(t.Text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cypher parser: invalid integer %q", t.Text)
		}
		return n, nil
	case tokFloat:
		p.advance()
		f, err := strconv.ParseFloat(t.Text, 64)
		if err != nil {
			return nil, fmt.Errorf("cypher parser: invalid float %q", t.Text)
		}
		return f, nil
	case tokTrue:
		p.advance()
		return true, nil
	case tokFalse:
		p.advance()
		return false, nil
	case tokNull:
		p.advance()
		return nil, nil
	default:
		return nil, fmt.Errorf("cypher parser: expected literal, got %s at position %d",
			tokenKindName(t.Kind), t.Pos)
	}
}

// parseRelPattern parses a relationship pattern:
//
//	-[r:FOLLOWS*1..3]->   (outgoing)
//	<-[r:FOLLOWS]-        (incoming)
//	-[r:FOLLOWS]-         (undirected / both)
func (p *parser) parseRelPattern() (RelPattern, error) {
	rp := RelPattern{
		Dir:     Outgoing,
		MinHops: 1,
		MaxHops: 1,
	}

	// Determine direction prefix.
	leftArrow := false
	if p.is(tokLArrow) {
		// <-
		leftArrow = true
		p.advance() // consume <-
	} else if p.is(tokDash) {
		p.advance() // consume -
	} else {
		return rp, fmt.Errorf("cypher parser: expected '-' or '<-' at position %d", p.cur().Pos)
	}

	// '[' ... ']'
	if _, err := p.expect(tokLBracket); err != nil {
		return rp, err
	}

	// Optional variable: [r ...]
	if p.is(tokIdent) {
		rp.Variable = p.advance().Text
	}

	// Optional label: [:FOLLOWS ...]
	if p.is(tokColon) {
		p.advance()
		labelTok, err := p.expect(tokIdent)
		if err != nil {
			return rp, fmt.Errorf("cypher parser: expected relationship label after ':'")
		}
		rp.Label = labelTok.Text
	}

	// Optional variable-length: *min..max
	if p.is(tokStar) {
		p.advance()
		rp.VarLength = true
		rp.MinHops = 1
		rp.MaxHops = -1 // unlimited by default

		// Optional min
		if p.is(tokInt) {
			n, _ := strconv.Atoi(p.advance().Text)
			rp.MinHops = n
			rp.MaxHops = n // if no .., max = min
		}

		// Optional ..max
		if p.is(tokDotDot) {
			p.advance()
			rp.MaxHops = -1 // unlimited if no max specified
			if p.is(tokInt) {
				n, _ := strconv.Atoi(p.advance().Text)
				rp.MaxHops = n
			}
		}
	}

	if _, err := p.expect(tokRBracket); err != nil {
		return rp, err
	}

	// Direction suffix: -> or -
	if leftArrow {
		// <-[...]-  → expect a dash
		if _, err := p.expect(tokDash); err != nil {
			return rp, fmt.Errorf("cypher parser: expected '-' to close '<-[...]-' pattern")
		}
		rp.Dir = Incoming
	} else {
		// -[...]-> or -[...]-
		if p.is(tokArrow) {
			p.advance()
			rp.Dir = Outgoing
		} else if p.is(tokDash) {
			p.advance()
			rp.Dir = Both
		} else {
			return rp, fmt.Errorf("cypher parser: expected '->' or '-' after relationship pattern at position %d", p.cur().Pos)
		}
	}

	return rp, nil
}

// ---------------- RETURN --------------------------------------------------

func (p *parser) parseReturnClause() (ReturnClause, error) {
	rc := ReturnClause{}

	if _, err := p.expect(tokReturn); err != nil {
		return rc, err
	}

	for {
		item, err := p.parseReturnItem()
		if err != nil {
			return rc, err
		}
		rc.Items = append(rc.Items, item)

		if !p.match(tokComma) {
			break
		}
	}

	return rc, nil
}

func (p *parser) parseReturnItem() (ReturnItem, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return ReturnItem{}, err
	}
	ri := ReturnItem{Expr: expr}

	// Optional: AS alias
	if p.is(tokAs) {
		p.advance()
		aliasTok, err := p.expect(tokIdent)
		if err != nil {
			return ri, fmt.Errorf("cypher parser: expected alias after AS")
		}
		ri.Alias = aliasTok.Text
	}

	return ri, nil
}

// ---------------- ORDER BY ------------------------------------------------

func (p *parser) parseOrderItems() ([]OrderItem, error) {
	var items []OrderItem
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		item := OrderItem{Expr: expr}
		if p.is(tokDesc) {
			p.advance()
			item.Desc = true
		} else if p.is(tokAsc) {
			p.advance()
		}
		items = append(items, item)
		if !p.match(tokComma) {
			break
		}
	}
	return items, nil
}

// ---------------- expressions ---------------------------------------------

// parseExpr → parseOrExpr
func (p *parser) parseExpr() (Expression, error) {
	return p.parseOrExpr()
}

// parseOrExpr → parseAndExpr (OR parseAndExpr)*
func (p *parser) parseOrExpr() (Expression, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return Expression{}, err
	}

	if !p.is(tokOr) {
		return left, nil
	}

	operands := []Expression{left}
	for p.match(tokOr) {
		right, err := p.parseAndExpr()
		if err != nil {
			return Expression{}, err
		}
		operands = append(operands, right)
	}
	return orExpr(operands...), nil
}

// parseAndExpr → parseNotExpr (AND parseNotExpr)*
func (p *parser) parseAndExpr() (Expression, error) {
	left, err := p.parseNotExpr()
	if err != nil {
		return Expression{}, err
	}

	if !p.is(tokAnd) {
		return left, nil
	}

	operands := []Expression{left}
	for p.match(tokAnd) {
		right, err := p.parseNotExpr()
		if err != nil {
			return Expression{}, err
		}
		operands = append(operands, right)
	}
	return andExpr(operands...), nil
}

// parseNotExpr → [NOT] parseComparison
func (p *parser) parseNotExpr() (Expression, error) {
	if p.match(tokNot) {
		inner, err := p.parseComparison()
		if err != nil {
			return Expression{}, err
		}
		return notExpr(inner), nil
	}
	return p.parseComparison()
}

// parseComparison → parsePrimary [ op parsePrimary ]
func (p *parser) parseComparison() (Expression, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return Expression{}, err
	}

	var op CompOp
	switch p.cur().Kind {
	case tokEq:
		op = OpEq
	case tokNeq:
		op = OpNeq
	case tokLt:
		op = OpLt
	case tokGt:
		op = OpGt
	case tokLte:
		op = OpLte
	case tokGte:
		op = OpGte
	default:
		return left, nil // no comparison operator — just return primary
	}
	p.advance() // consume operator

	right, err := p.parsePrimary()
	if err != nil {
		return Expression{}, err
	}

	return compExpr(left, op, right), nil
}

// parsePrimary parses:
//
//	ident '.' ident    → PropAccess
//	ident '(' expr ')' → FuncCall
//	ident              → VarRef
//	literal            → Literal
func (p *parser) parsePrimary() (Expression, error) {
	t := p.cur()

	// Identifier-led expressions.
	if t.Kind == tokIdent || t.Kind == tokType {
		name := t.Text
		p.advance()

		// ident '.' ident → property access
		if p.is(tokDot) {
			p.advance()
			propTok, err := p.expect(tokIdent)
			if err != nil {
				return Expression{}, fmt.Errorf("cypher parser: expected property name after '.'")
			}
			return propExpr(name, propTok.Text), nil
		}

		// ident '(' expr ')' → function call
		if p.is(tokLParen) {
			p.advance()
			var args []Expression
			if !p.is(tokRParen) {
				arg, err := p.parseExpr()
				if err != nil {
					return Expression{}, err
				}
				args = append(args, arg)
				for p.match(tokComma) {
					arg, err := p.parseExpr()
					if err != nil {
						return Expression{}, err
					}
					args = append(args, arg)
				}
			}
			if _, err := p.expect(tokRParen); err != nil {
				return Expression{}, err
			}
			return funcCallExpr(name, args...), nil
		}

		// plain ident → variable reference
		return varRefExpr(name), nil
	}

	// Parameter reference: $paramName
	if t.Kind == tokParam {
		p.advance()
		return Expression{Kind: ExprParam, ParamName: t.Text}, nil
	}

	// Literal values.
	switch t.Kind {
	case tokString:
		p.advance()
		return litExpr(t.Text), nil
	case tokInt:
		p.advance()
		n, _ := strconv.ParseInt(t.Text, 10, 64)
		return litExpr(n), nil
	case tokFloat:
		p.advance()
		f, _ := strconv.ParseFloat(t.Text, 64)
		return litExpr(f), nil
	case tokTrue:
		p.advance()
		return litExpr(true), nil
	case tokFalse:
		p.advance()
		return litExpr(false), nil
	case tokNull:
		p.advance()
		return litExpr(nil), nil
	}

	return Expression{}, fmt.Errorf("cypher parser: unexpected token %s at position %d",
		tokenKindName(t.Kind), t.Pos)
}
