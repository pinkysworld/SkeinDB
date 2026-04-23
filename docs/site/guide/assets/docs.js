/* Docs site interactions: search filter, active-TOC highlight, keyboard shortcut */
(function(){
  const search = document.getElementById('docsSearch');
  if (search) {
    search.addEventListener('input', e => {
      const q = e.target.value.trim().toLowerCase();
      document.querySelectorAll('.sidebar-nav li').forEach(li => {
        const text = li.textContent.toLowerCase();
        li.classList.toggle('hidden', q && !text.includes(q));
      });
    });
    document.addEventListener('keydown', e => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        search.focus();
        search.select();
      }
    });
  }
  // Highlight active TOC entry on scroll
  const toc = document.getElementById('toc');
  if (toc) {
    const links = Array.from(toc.querySelectorAll('a[href^="#"]'));
    const map = new Map();
    links.forEach(a => {
      const id = decodeURIComponent(a.getAttribute('href').slice(1));
      const el = document.getElementById(id);
      if (el) map.set(el, a);
    });
    if (map.size) {
      const io = new IntersectionObserver(entries => {
        entries.forEach(en => {
          if (en.isIntersecting) {
            links.forEach(l => l.classList.remove('active'));
            const a = map.get(en.target);
            if (a) a.classList.add('active');
          }
        });
      }, {rootMargin: '-20% 0px -70% 0px'});
      map.forEach((_a, el) => io.observe(el));
    }
  }
})();
