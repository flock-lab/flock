// Adds the web accessibility link to the bottom of every page

function AddAccessibilityToMainDiv()
{
    var main_div = document.getElementsByClassName("main")[0];
    var h = document.createElement('div');
    h.setAttribute("class", "accessibility-link");
    h.innerHTML = "<a href=\"https://www.umd.edu/web-accessibility\" title=\"UMD Web Accessibility\">Web Accessibility</a>";
    main_div.insertBefore(h, main_div.lastChild);
}
AddOnLoad(AddAccessibilityToMainDiv);
