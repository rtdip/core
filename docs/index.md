---
hide:
  - navigation
  - toc
---
<style>
[data-md-color-scheme="slate"] .md-main{
  background: url(images/background-curve-slate.svg) no-repeat bottom,
    linear-gradient(
      to bottom,
      var(--md-primary-fg-color),
      var(--md-primary-mg-color) 99%,
      hsla(200, 18%, 26%, 1)
    );
}

[data-md-color-scheme="default"] .md-main{
  background: url(images/background-curve-default.svg) no-repeat bottom,
    linear-gradient(
      to bottom,
      var(--md-primary-fg-color),
      var(--md-primary-mg-color) 99%,
      hsla(200, 18%, 26%, 1)
    );
}

.row {
  width: 100%;
  display: flex;
}

.block {
  width: 100%;
  display:flex;
  color: var(--md-primary-text-slate) !important;
}

.block h1{
  color: var(--md-primary-text-slate) !important;
  font-weight: 700;
  margin-bottom: px2rem(20px);
}

.md-button {
  margin: 5px;
  margin-left:0px;
  color: var(--md-primary-text-slate) !important;
  border-color: var(--md-primary-text-slate) !important;
  width: 100%
  }

 .md-button:focus, .md-button:hover {
	 color: var(--md-accent-bg-color);
	 background-color: var(--md-accent-fg-color);
	 border-color: var(--md-accent-fg-color) !important;
}

.md-footer {
  background-color: white;
  color: var(--md-primary-bg-slate);
}

[data-md-color-scheme="slate"] .md-footer {
  background-color: var(--md-primary-bg-slate);
  color: white;
}
</style>


<div class="row">
    <div class="block">
      <div class="block_content">
        <h1>Welcome to the Real Time Data Ingestion Platform</h1>
        <p>Easy access to high volume, historical and real time process data for analytics applications, engineers, and data scientists wherever they are.</p>
        <div class="block">
          <div class="row"> 
            <div class="col">
              <a
                href="getting-started/installation/"
                title="Getting Started"
                class="md-button"
              >
                Getting Started
              </a>
            </div>
          </div>
        </div>
      </div>
      <div class="block_image">
        <img
            src="assets/illustration.png"
            alt=""
            width="1659"
            height="1200"
            draggable="false"
        >
      </div>
  </div>
</div>
