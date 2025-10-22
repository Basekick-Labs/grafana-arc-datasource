# Contributing to Arc Grafana Datasource

Thank you for your interest in contributing to the Arc Grafana Datasource!

## Development Setup

### Prerequisites

- Node.js 18+
- Go 1.21+
- Mage (Go build tool): `go install github.com/magefile/mage@latest`
- Grafana 10.0+ (for testing)

### Getting Started

1. **Fork and Clone**
   ```bash
   git clone https://github.com/<your-username>/grafana-arc-datasource
   cd grafana-arc-datasource
   ```

2. **Install Dependencies**
   ```bash
   # Frontend
   npm install

   # Backend
   go mod download
   ```

3. **Build Plugin**
   ```bash
   # Frontend
   npm run dev

   # Backend (in another terminal)
   mage -v
   ```

4. **Install to Grafana**
   ```bash
   # Create symlink for development
   ln -s $(pwd)/dist /var/lib/grafana/plugins/grafana-arc-datasource

   # Restart Grafana
   systemctl restart grafana-server
   ```

## Development Workflow

### Frontend Changes

1. Edit files in `src/`
2. Run `npm run dev` for hot reload
3. Refresh Grafana to see changes

### Backend Changes

1. Edit files in `pkg/`
2. Run `mage build`
3. Restart Grafana backend: `systemctl restart grafana-server`

### Testing

```bash
# Frontend tests
npm run test

# Backend tests
go test ./pkg/...

# Linting
npm run lint
go fmt ./...
```

## Submitting Changes

### Pull Request Process

1. Create a feature branch
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes
   - Follow existing code style
   - Add tests for new features
   - Update documentation

3. Commit with clear messages
   ```bash
   git commit -m "feat: add SQL syntax highlighting"
   ```

4. Push and create PR
   ```bash
   git push origin feature/your-feature-name
   ```

5. Fill out PR template
   - Describe changes
   - Reference any issues
   - Add screenshots if UI changes

### Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Test additions/changes
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `chore:` - Build/tooling changes

Examples:
```
feat: add query result caching
fix: resolve Arrow timestamp parsing issue
docs: update installation instructions
test: add Arrow type conversion tests
```

## Code Style

### TypeScript/React

- Use functional components with hooks
- Follow Grafana UI component patterns
- Use TypeScript strict mode
- Format with Prettier

### Go

- Follow Go standard style
- Use `gofmt` for formatting
- Add comments for exported functions
- Handle errors explicitly

## Adding New Features

### Frontend Features

1. Add types to `src/types.ts`
2. Implement component in `src/`
3. Add to `src/module.ts` if needed
4. Write tests
5. Update README

### Backend Features

1. Add to appropriate file in `pkg/plugin/`
2. Update Go types if needed
3. Write tests
4. Update documentation

## Testing Guidelines

### Frontend Tests

- Test component rendering
- Test user interactions
- Test edge cases
- Mock external dependencies

### Backend Tests

- Test Arrow parsing with various types
- Test macro expansion
- Test error handling
- Test type conversions

### Integration Tests

Manual testing checklist:
- [ ] Install plugin in Grafana
- [ ] Configure datasource
- [ ] Test "Save & Test" button
- [ ] Create dashboard
- [ ] Write queries with macros
- [ ] Test variable queries
- [ ] Verify visualizations render
- [ ] Test error scenarios

## Documentation

When adding features, update:
- `README.md` - User-facing documentation
- `ARCHITECTURE.md` - Technical details
- Inline code comments
- TypeScript/Go doc comments

## Release Process

Releases are handled by maintainers:

1. Version bump in `package.json` and `plugin.json`
2. Update CHANGELOG.md
3. Create GitHub release
4. Build and sign plugin
5. Submit to Grafana marketplace

## Getting Help

- GitHub Issues: Bug reports and feature requests
- Discussions: Questions and community support
- Pull Requests: Code contributions

## Code of Conduct

Be respectful and inclusive. We welcome contributions from everyone.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
