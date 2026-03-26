/**
 * Mermaid Diagram Pan & Zoom
 *
 * Material for MkDocs renders Mermaid diagrams client-side:
 * 1. Static HTML has: <pre class="mermaid"><code>graph TD...</code></pre>
 * 2. Mermaid.js replaces the <code> with an <svg>
 * 3. This script detects the SVG and wraps it in a pan/zoom container
 */

(function () {
  "use strict";

  var MIN_SCALE = 0.3;
  var MAX_SCALE = 5;
  var ZOOM_STEP = 0.15;

  function createButton(text, title) {
    var btn = document.createElement("button");
    btn.className = "mermaid-zoom-btn";
    btn.textContent = text;
    btn.title = title;
    return btn;
  }

  function initZoom(svg) {
    // Mark so we don't double-init
    svg.setAttribute("data-zoom-init", "true");

    // Material replaces <pre class="mermaid"> with a bare <svg>.
    // Wrap the SVG itself in a zoom container.
    var wrapper = document.createElement("div");
    wrapper.className = "mermaid-zoom-wrapper";
    svg.parentNode.insertBefore(wrapper, svg);

    var viewport = document.createElement("div");
    viewport.className = "mermaid-zoom-viewport";
    wrapper.appendChild(viewport);
    viewport.appendChild(svg);

    var scale = 1;
    var panX = 0;
    var panY = 0;
    var isPanning = false;
    var startX = 0;
    var startY = 0;

    function applyTransform() {
      svg.style.transform =
        "translate(" + panX + "px, " + panY + "px) scale(" + scale + ")";
    }

    function resetView() {
      scale = 1;
      panX = 0;
      panY = 0;
      applyTransform();
    }

    // Mouse wheel zoom
    wrapper.addEventListener(
      "wheel",
      function (e) {
        e.preventDefault();
        scale = e.deltaY < 0
          ? Math.min(MAX_SCALE, scale + ZOOM_STEP)
          : Math.max(MIN_SCALE, scale - ZOOM_STEP);
        applyTransform();
      },
      { passive: false }
    );

    // Pan via mouse drag
    wrapper.addEventListener("mousedown", function (e) {
      if (e.button !== 0) return;
      isPanning = true;
      startX = e.clientX - panX;
      startY = e.clientY - panY;
      wrapper.style.cursor = "grabbing";
      e.preventDefault();
    });

    document.addEventListener("mousemove", function (e) {
      if (!isPanning) return;
      panX = e.clientX - startX;
      panY = e.clientY - startY;
      applyTransform();
    });

    document.addEventListener("mouseup", function () {
      if (!isPanning) return;
      isPanning = false;
      wrapper.style.cursor = "grab";
    });

    // Double-click to reset
    wrapper.addEventListener("dblclick", function (e) {
      e.preventDefault();
      resetView();
    });

    // Touch: pinch zoom + drag pan
    var lastTouchDist = 0;

    wrapper.addEventListener("touchstart", function (e) {
      if (e.touches.length === 1) {
        isPanning = true;
        startX = e.touches[0].clientX - panX;
        startY = e.touches[0].clientY - panY;
      } else if (e.touches.length === 2) {
        lastTouchDist = Math.hypot(
          e.touches[0].clientX - e.touches[1].clientX,
          e.touches[0].clientY - e.touches[1].clientY
        );
      }
    }, { passive: true });

    wrapper.addEventListener("touchmove", function (e) {
      if (e.touches.length === 1 && isPanning) {
        panX = e.touches[0].clientX - startX;
        panY = e.touches[0].clientY - startY;
        applyTransform();
      } else if (e.touches.length === 2) {
        var dist = Math.hypot(
          e.touches[0].clientX - e.touches[1].clientX,
          e.touches[0].clientY - e.touches[1].clientY
        );
        if (lastTouchDist > 0) {
          scale = Math.max(MIN_SCALE, Math.min(MAX_SCALE, scale * (dist / lastTouchDist)));
          applyTransform();
        }
        lastTouchDist = dist;
      }
    }, { passive: true });

    wrapper.addEventListener("touchend", function () {
      isPanning = false;
      lastTouchDist = 0;
    }, { passive: true });

    // Zoom buttons
    var controls = document.createElement("div");
    controls.className = "mermaid-zoom-controls";

    var btnIn = createButton("+", "Zoom in");
    var btnOut = createButton("\u2212", "Zoom out");
    var btnReset = createButton("Reset", "Reset view (or double-click)");
    btnReset.classList.add("mermaid-zoom-reset");

    btnIn.addEventListener("click", function (e) { e.stopPropagation(); scale = Math.min(MAX_SCALE, scale + ZOOM_STEP); applyTransform(); });
    btnOut.addEventListener("click", function (e) { e.stopPropagation(); scale = Math.max(MIN_SCALE, scale - ZOOM_STEP); applyTransform(); });
    btnReset.addEventListener("click", function (e) { e.stopPropagation(); resetView(); });

    controls.appendChild(btnIn);
    controls.appendChild(btnOut);
    controls.appendChild(btnReset);
    wrapper.appendChild(controls);

    wrapper.style.cursor = "grab";
    svg.style.transformOrigin = "center center";
  }

  function scan() {
    // Material for MkDocs replaces <pre class="mermaid"> entirely with
    // a bare <svg id="__mermaid_N"> at the same DOM position.
    // Target these SVGs by their id prefix.
    var svgs = document.querySelectorAll("svg[id^='__mermaid']:not([data-zoom-init])");
    for (var i = 0; i < svgs.length; i++) {
      initZoom(svgs[i]);
    }
  }

  // Material for MkDocs renders mermaid async after page load.
  // Use both MutationObserver and polling to catch the SVGs.

  // 1. MutationObserver — fires when Mermaid injects SVGs
  function startObserver() {
    var obs = new MutationObserver(scan);
    obs.observe(document.body, { childList: true, subtree: true });
  }

  // 2. Polling fallback — in case observer misses it
  var pollCount = 0;
  var pollMax = 50;
  var pollTimer = setInterval(function () {
    scan();
    pollCount++;
    if (pollCount >= pollMax) clearInterval(pollTimer);
  }, 200);

  // Start
  if (document.body) {
    startObserver();
    scan();
  } else {
    document.addEventListener("DOMContentLoaded", function () {
      startObserver();
      scan();
    });
  }
})();
